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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/accelerator/nvidia"
	"github.com/kaito-project/kaito/pkg/consts"
)

const (
	AnnotationPrefix = "quota.kaito.sh/"

	AnnotationTeam             = AnnotationPrefix + "team"
	AnnotationPriority         = AnnotationPrefix + "priority"
	AnnotationMaxNodes         = AnnotationPrefix + "max-nodes"
	AnnotationMaxGPUMilli      = AnnotationPrefix + "max-gpu-milli"
	AnnotationMaxWait          = AnnotationPrefix + "max-wait"
	AnnotationWholeGPUs        = AnnotationPrefix + "whole-gpus"
	AnnotationSliceGranularity = AnnotationPrefix + "slice-granularity"
	AnnotationYieldable        = AnnotationPrefix + "yieldable"
	AnnotationState            = AnnotationPrefix + "state"

	ConfigMapName = "kaito-quota-config"
	ConfigMapKey  = "config.json"
)

// PersistedState is the cluster-object-backed arbitration state. It is stored
// in an annotation so queue order, wait duration and preemption records survive
// controller restart without a database or in-memory queue.
type PersistedState struct {
	Phase        Phase            `json:"phase,omitempty"`
	WaitSince    time.Time        `json:"waitSince,omitempty"`
	LastLimit    *EffectiveLimit  `json:"lastLimit,omitempty"`
	Preemption   *PreemptionState `json:"preemption,omitempty"`
	BlockedUntil time.Time        `json:"blockedUntil,omitempty"`
}

// Policy is the optional ConfigMap quota policy.
type Policy struct {
	DefaultLimit    LimitConfig            `json:"defaultLimit,omitempty"`
	Teams           map[string]LimitConfig `json:"teams,omitempty"`
	PreemptionGrace string                 `json:"preemptionGrace,omitempty"`
}

// LimitConfig is the parseable JSON form of Limit.
type LimitConfig struct {
	MaxNodes    *int   `json:"maxNodes,omitempty"`
	MaxGPUMilli *int64 `json:"maxGpuMilli,omitempty"`
	MaxWait     string `json:"maxWait,omitempty"`
}

// Limit converts config and parses its duration.
func (c LimitConfig) Limit() (Limit, error) {
	l := Limit{MaxNodes: c.MaxNodes, MaxGPUMilli: c.MaxGPUMilli}
	if c.MaxWait != "" {
		d, err := time.ParseDuration(c.MaxWait)
		if err != nil {
			return Limit{}, fmt.Errorf("invalid maxWait %q: %w", c.MaxWait, err)
		}
		l.MaxWait = d
	}
	return l, nil
}

// PolicyFromConfigMap parses the optional quota ConfigMap.
func PolicyFromConfigMap(cm *corev1.ConfigMap) (Policy, error) {
	var p Policy
	if cm == nil {
		return p, nil
	}
	raw, ok := cm.Data[ConfigMapKey]
	if !ok {
		// Also accept the conventional .json/.yaml key without pulling a YAML
		// parser into the arbitration core.
		for key, value := range cm.Data {
			if key == "policy.json" {
				raw = value
				break
			}
		}
	}
	if raw == "" {
		return p, nil
	}
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return Policy{}, fmt.Errorf("parse quota policy: %w", err)
	}
	if _, err := p.DefaultLimit.Limit(); err != nil {
		return Policy{}, fmt.Errorf("default quota: %w", err)
	}
	for team, cfg := range p.Teams {
		if _, err := cfg.Limit(); err != nil {
			return Policy{}, fmt.Errorf("team %q quota: %w", team, err)
		}
	}
	return p, nil
}

// EncodeState serializes state for an annotation.
func EncodeState(state PersistedState) (string, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// DecodeState reads persisted state. A missing annotation is not an error.
func DecodeState(annotations map[string]string) (PersistedState, error) {
	var state PersistedState
	raw := annotations[AnnotationState]
	if raw == "" {
		return state, nil
	}
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return PersistedState{}, fmt.Errorf("parse %s annotation: %w", AnnotationState, err)
	}
	return state, nil
}

// WorkloadFromWorkspace reconstructs an arbitration Workload from the cluster
// object. The object, not process memory, remains the source of truth.
func WorkloadFromWorkspace(ws *kaitov1beta1.Workspace, now time.Time) (Workload, error) {
	if ws == nil {
		return Workload{}, fmt.Errorf("workspace is nil")
	}
	key := ws.Namespace + "/" + ws.Name
	w := Workload{
		Key:       key,
		CreatedAt: ws.CreationTimestamp.Time,
		Deleting:  !ws.DeletionTimestamp.IsZero(),
	}
	if team := ws.Annotations[AnnotationTeam]; team != "" {
		w.Team = team
	}
	if raw := ws.Annotations[AnnotationPriority]; raw != "" {
		priority, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			w.Layers = append(w.Layers, Layer{Source: SourceWorkload, Err: fmt.Errorf("invalid %s %q: %w", AnnotationPriority, raw, err)})
		} else {
			w.Priority = int32(priority)
		}
	}

	if raw := ws.Annotations[AnnotationYieldable]; raw != "" {
		if yieldable, err := strconv.ParseBool(raw); err == nil {
			w.Yieldable = yieldable
		} else {
			w.Layers = append(w.Layers, Layer{Source: SourceWorkload, Err: fmt.Errorf("invalid %s %q: %w", AnnotationYieldable, raw, err)})
		}
	}

	w.Demand = DemandFromWorkspace(ws)

	workloadLimit := Limit{}
	hasWorkloadLimit := false
	if raw := ws.Annotations[AnnotationMaxNodes]; raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			w.Layers = append(w.Layers, Layer{Source: SourceWorkload, Err: fmt.Errorf("invalid %s %q: %w", AnnotationMaxNodes, raw, err)})
		} else {
			workloadLimit.MaxNodes = &v
			hasWorkloadLimit = true
		}
	}
	if raw := ws.Annotations[AnnotationMaxGPUMilli]; raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			w.Layers = append(w.Layers, Layer{Source: SourceWorkload, Err: fmt.Errorf("invalid %s %q: %w", AnnotationMaxGPUMilli, raw, err)})
		} else {
			workloadLimit.MaxGPUMilli = &v
			hasWorkloadLimit = true
		}
	}
	if raw := ws.Annotations[AnnotationMaxWait]; raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			w.Layers = append(w.Layers, Layer{Source: SourceWorkload, Err: fmt.Errorf("invalid %s %q: %w", AnnotationMaxWait, raw, err)})
		} else {
			workloadLimit.MaxWait = d
			hasWorkloadLimit = true
		}
	}
	if hasWorkloadLimit {
		w.Layers = append(w.Layers, Layer{Source: SourceWorkload, Limit: workloadLimit})
	}

	state, err := DecodeState(ws.Annotations)
	if err != nil {
		w.Layers = append(w.Layers, Layer{Source: SourceWorkload, Err: err})
	} else {
		w.Phase = state.Phase
		w.WaitSince = state.WaitSince
		w.LastEffective = state.LastLimit
		w.Preemption = state.Preemption
		w.PreemptionBlockedUntil = state.BlockedUntil
	}
	if w.Phase == "" {
		// Objects that predate the state annotation but are already Ready are
		// existing occupants. New objects are queued until the gate persists
		// their first decision.
		if ws.Status.State == kaitov1beta1.WorkspaceStateReady {
			w.Phase = PhaseAdmitted
			w.Ready = true
		} else {
			w.Phase = PhasePending
		}
	}
	if ws.Status.State == kaitov1beta1.WorkspaceStateReady {
		w.Ready = true
		w.NodeReady = true
	}
	if cond := meta.FindStatusCondition(ws.Status.Conditions, string(kaitov1beta1.ConditionTypeNodeStatus)); cond != nil &&
		cond.Status == metav1.ConditionTrue {
		w.NodeReady = true
	}
	w.Failed = ws.Status.State == kaitov1beta1.WorkspaceStateFailed
	if !w.Failed {
		for i := range ws.Status.Conditions {
			cond := ws.Status.Conditions[i]
			if cond.Status == metav1.ConditionFalse && consts.IsTerminatedReason(cond.Reason) {
				w.Failed = true
				break
			}
		}
	}
	return w, nil
}

// DemandFromWorkspace maps the existing estimator result and partition shape to
// separately metered node, whole-card and slice demand.
func DemandFromWorkspace(ws *kaitov1beta1.Workspace) Demand {
	d := Demand{}
	if ws.Status.TargetNodeCount > 0 {
		d.Nodes = int(ws.Status.TargetNodeCount)
	} else if ws.Resource.Count != nil { //nolint:staticcheck // preserved deprecated fallback
		d.Nodes = *ws.Resource.Count
	}
	if raw := ws.Annotations[AnnotationWholeGPUs]; raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			d.WholeGPUCards = v
		}
	}
	if p := ws.Resource.Partition; p != nil {
		switch p.Mode {
		case kaitov1beta1.PartitionModeAccelerator:
			if d.WholeGPUCards == 0 && p.Count != nil {
				d.WholeGPUCards = *p.Count
			}
		case kaitov1beta1.PartitionModeMIG:
			s := SliceDemand{Profile: p.Profile, Count: 1, Granularity: 7, SlotsPerSlice: 1}
			if computeSlots, _, err := nvidia.ParseMIGProfile(p.Profile); err == nil && computeSlots > 0 {
				s.SlotsPerSlice = computeSlots
			}
			if raw := ws.Annotations[AnnotationSliceGranularity]; raw != "" {
				if g, err := strconv.Atoi(raw); err == nil && g > 0 {
					s.Granularity = g
				}
			}
			d.Slices = append(d.Slices, s)
		}
	}
	return d
}

// SnapshotFromWorkspaces builds a deterministic arbitration snapshot.
func SnapshotFromWorkspaces(wsList []kaitov1beta1.Workspace, policy Policy, now time.Time) (Snapshot, error) {
	s := Snapshot{Now: now, Teams: map[string]Limit{}}
	if l, err := policy.DefaultLimit.Limit(); err != nil {
		return s, err
	} else {
		s.Cluster = l
	}
	for team, cfg := range policy.Teams {
		l, err := cfg.Limit()
		if err != nil {
			return s, err
		}
		s.Teams[team] = l
	}
	if policy.PreemptionGrace != "" {
		d, err := time.ParseDuration(policy.PreemptionGrace)
		if err != nil {
			return s, fmt.Errorf("invalid preemptionGrace: %w", err)
		}
		s.PreemptionGrace = d
	}
	for i := range wsList {
		w, err := WorkloadFromWorkspace(&wsList[i], now)
		if err != nil {
			return s, err
		}
		s.Workloads = append(s.Workloads, w)
	}
	return s, nil
}
