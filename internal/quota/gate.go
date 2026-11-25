// Copyright (c) KAITO authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quota

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
)

// NodeReleaser is the subset of NodeProvisioner used to return machine capacity
// when a Workspace is preempted, rejected after wait timeout, or rolled back.
type NodeReleaser interface {
	DeleteNodes(context.Context, *kaitov1beta1.Workspace) error
}

// Gate is the controller-facing admission boundary. It serializes arbitration,
// rebuilds the ledger from Workspace objects, persists state on those objects,
// and releases node claims when quota is reclaimed.
type Gate struct {
	Client   client.Client
	Nodes    NodeReleaser
	Recorder record.EventRecorder
	Metrics  *Metrics

	// Namespace holds the optional cluster policy ConfigMap and arbitration
	// lock. When empty, the current Workspace namespace is used.
	Namespace string

	Now func() time.Time
}

// NewGate constructs a Gate.
func NewGate(c client.Client, namespace string, nodes NodeReleaser, recorder record.EventRecorder) *Gate {
	return &Gate{Client: c, Namespace: namespace, Nodes: nodes, Recorder: recorder, Metrics: DefaultMetrics()}
}

func (g *Gate) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now()
}

// Admit arbitrates all Workspaces and returns the decision for ws. A nil
// decision means quota is not configured and the caller must preserve today's
// provisioning path exactly.
func (g *Gate) Admit(ctx context.Context, ws *kaitov1beta1.Workspace) (*Decision, error) {
	if g == nil || g.Client == nil || ws == nil {
		return nil, nil
	}
	now := g.now()
	policyNS := g.Namespace
	if policyNS == "" {
		policyNS = ws.Namespace
	}

	policy, policyErr := g.loadPolicy(ctx, policyNS)
	if policyErr != nil {
		// Do not return before arbitration: per-object last-known limits can
		// keep the previous effective value while the invalid source is marked.
		policy = Policy{}
	}
	if !policyConfigured(policy) && !workspaceOptsIn(ws) && policyErr == nil {
		return nil, nil
	}

	lock := Lock{Client: g.Client, Namespace: policyNS, Now: g.Now}
	release, err := lock.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = release(context.Background()) }()

	var list kaitov1beta1.WorkspaceList
	if err := g.Client.List(ctx, &list); err != nil {
		return nil, err
	}

	snapshot, err := SnapshotFromWorkspaces(list.Items, policy, now)
	if err != nil && policyErr == nil {
		return nil, err
	}
	if policyErr != nil {
		snapshot.Now = now
		snapshot.ClusterErr = policyErr
		if snapshot.Teams == nil {
			snapshot.Teams = map[string]Limit{}
		}
		if len(snapshot.Workloads) == 0 {
			for i := range list.Items {
				w, fromErr := WorkloadFromWorkspace(&list.Items[i], now)
				if fromErr != nil {
					return nil, fromErr
				}
				snapshot.Workloads = append(snapshot.Workloads, w)
			}
		}
	}
	plan := Arbitrate(snapshot)

	g.recordWaitingMetrics(snapshot.Workloads, plan)

	for i := range list.Items {
		obj := &list.Items[i]
		if !obj.DeletionTimestamp.IsZero() {
			continue
		}
		key := obj.Namespace + "/" + obj.Name
		d, ok := plan.For(key)
		if !ok {
			continue
		}
		previous, _ := DecodeState(obj.Annotations)
		if err := g.reconcileObject(ctx, obj, d, previous); err != nil {
			return nil, err
		}
	}

	currentKey := ws.Namespace + "/" + ws.Name
	d, ok := plan.For(currentKey)
	if !ok {
		return nil, nil
	}
	return &d, nil
}

func (g *Gate) loadPolicy(ctx context.Context, namespace string) (Policy, error) {
	cm := &corev1.ConfigMap{}
	err := g.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ConfigMapName}, cm)
	if apierrors.IsNotFound(err) {
		return Policy{}, nil
	}
	if err != nil {
		return Policy{}, err
	}
	return PolicyFromConfigMap(cm)
}

func policyConfigured(p Policy) bool {
	if p.DefaultLimit.MaxNodes != nil || p.DefaultLimit.MaxGPUMilli != nil || p.DefaultLimit.MaxWait != "" {
		return true
	}
	for _, team := range p.Teams {
		if team.MaxNodes != nil || team.MaxGPUMilli != nil || team.MaxWait != "" {
			return true
		}
	}
	return false
}

func workspaceOptsIn(ws *kaitov1beta1.Workspace) bool {
	// Only a workload-declared limit activates the gate without cluster
	// policy. Team, priority and persisted state alone do not constrain an
	// unlimited cluster and therefore must not change legacy status behavior.
	for _, key := range []string{
		AnnotationMaxNodes, AnnotationMaxGPUMilli, AnnotationMaxWait,
	} {
		if _, ok := ws.Annotations[key]; ok {
			return true
		}
	}
	return false
}

func (g *Gate) recordWaitingMetrics(workloads []Workload, plan Plan) {
	if g.Metrics == nil {
		return
	}
	counts := map[string]map[string]int{}
	previous := map[string]Phase{}
	teams := map[string]string{}
	for _, w := range workloads {
		previous[w.Key] = w.Phase
		teams[w.Key] = w.Team
	}
	for _, d := range plan.Decisions {
		team := teams[d.Key]
		if team == "" {
			team = "_unassigned"
		}
		g.Metrics.RecordRejectionTransition(team, d, previous[d.Key])
		if d.Phase != PhasePending && d.Phase != PhasePreempted {
			continue
		}
		reason := d.Reason
		if reason == "" {
			reason = "Unknown"
		}
		if counts[team] == nil {
			counts[team] = map[string]int{}
		}
		counts[team][reason]++
	}
	g.Metrics.SetWaiting(counts)
}

func (g *Gate) reconcileObject(ctx context.Context, ws *kaitov1beta1.Workspace, d Decision, previous PersistedState) error {
	// Reclaim machine capacity on transitions that release the reservation.
	if g.Nodes != nil {
		reclaim := false
		switch {
		case d.Phase == PhasePreempted && previous.Phase != PhasePreempted:
			reclaim = true
		case d.Phase == PhaseRejected && previous.Phase != PhaseRejected:
			reclaim = true
		case d.Reason == ReasonPreemptionRolledBack:
			reclaim = true
		}
		if reclaim {
			if err := g.Nodes.DeleteNodes(ctx, ws); err != nil {
				return err
			}
			if g.Recorder != nil {
				g.Recorder.Eventf(ws, corev1.EventTypeWarning, d.Reason, d.Message)
			}
		}
	}

	if g.Recorder != nil && previous.Phase != d.Phase {
		eventType := corev1.EventTypeNormal
		if d.Phase == PhaseRejected || d.Phase == PhasePreempted {
			eventType = corev1.EventTypeWarning
		}
		g.Recorder.Eventf(ws, eventType, d.Reason, d.Message)
	}

	state := PersistedState{
		Phase:        d.Phase,
		WaitSince:    d.WaitSince,
		BlockedUntil: d.BackoffUntil,
	}
	if d.EffectiveLimit.MaxNodes != 0 || d.EffectiveLimit.NodeSource != SourceNone {
		limit := d.EffectiveLimit
		state.LastLimit = &limit
	}
	if d.Preemption != nil {
		state.Preemption = d.Preemption
	} else if d.Phase == PhasePreempted && previous.Preemption != nil {
		state.Preemption = previous.Preemption
	}

	raw, err := EncodeState(state)
	if err != nil {
		return err
	}
	if ws.Annotations[AnnotationState] == raw {
		return nil
	}

	updated := ws.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[AnnotationState] = raw
	patch := client.MergeFromWithOptions(ws.DeepCopy(), client.MergeFromWithOptimisticLock{})
	return g.Client.Patch(ctx, updated, patch)
}
