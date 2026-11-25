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
	"fmt"
	"sort"
	"strings"
	"time"
)

// Phase is the arbitration phase persisted on a Workspace.
type Phase string

const (
	PhasePending   Phase = "Pending"
	PhaseAdmitted  Phase = "Admitted"
	PhasePreempted Phase = "Preempted"
	PhaseRejected  Phase = "Rejected"
)

// Stable reason strings. They intentionally do not reuse the cloud
// QuotaExceeded/CapacityUnavailable terminal reasons.
const (
	ReasonAdmitted               = "AdmissionGranted"
	ReasonClusterQuotaWaiting    = "ClusterQuotaInsufficient"
	ReasonTeamQuotaWaiting       = "TeamQuotaInsufficient"
	ReasonDemandExceedsLimit     = "DemandExceedsQuotaLimit"
	ReasonQuotaLimitZero         = "QuotaLimitIsZero"
	ReasonWaitTimeout            = "AdmissionWaitTimedOut"
	ReasonLimitResolutionFailed  = "QuotaLimitResolutionFailed"
	ReasonInvalidDemand          = "InvalidQuotaDemand"
	ReasonPreempted              = "PreemptedByHigherPriorityWorkspace"
	ReasonPreemptionGranted      = "HigherPriorityAdmittedWithPreemption"
	ReasonPreemptionRestored     = "PreemptedWorkspaceRestored"
	ReasonPreemptionRolledBack   = "PreemptionRolledBack"
	ReasonPreemptionBlockedAgain = "PreemptionBackoff"
)

// PreemptionState is the durable record needed to make preemption recoverable.
type PreemptionState struct {
	// Preemptor is the high-priority Workspace that owns this record. On a
	// victim record it is the preemptor; on the preemptor record Victims is set.
	Preemptor string `json:"preemptor,omitempty"`
	Victim    string `json:"victim,omitempty"`

	Victims  []string  `json:"victims,omitempty"`
	At       time.Time `json:"at"`
	Deadline time.Time `json:"deadline"`

	// Attempt is retained across rollback to prevent immediate repetition.
	Attempt int `json:"attempt,omitempty"`
}

// Workload is the deterministic input view of one cluster object.
type Workload struct {
	Key       string
	Team      string
	Demand    Demand
	Priority  int32
	CreatedAt time.Time

	Phase Phase

	// Deleting marks an object with a deletion timestamp. Its reservation is
	// gone immediately even while Kubernetes finalizers remove the object.
	Deleting bool

	// Ready reports whether the workload is serving/ready. A not-ready
	// admitted workload may be preempted even if it cannot yield a live serve.
	Ready bool
	// NodeReady reports whether the preemptor obtained its nodes. It is used
	// by the preemption grace/rollback state machine.
	NodeReady bool
	// Failed reports an observed terminal/failed Workspace state or condition.
	// Cloud failure attribution remains untouched; this only releases the
	// in-cluster quota reservation from the rebuilt ledger.
	Failed bool
	// Yieldable reports that a ready lower-priority workload may be wholly
	// yielded for preemption.
	Yieldable bool

	WaitSince time.Time

	// Layers are the typed quota layers attached to this reconciliation.
	Layers []Layer
	// LastEffective is the previously observed good limit. Invalid layers use
	// it instead of silently becoming unlimited.
	LastEffective *EffectiveLimit

	// Preemption is the durable preemption record, if one is in progress.
	Preemption *PreemptionState
	// PreemptionBlockedUntil prevents repeated preemption after rollback.
	PreemptionBlockedUntil time.Time
}

// Snapshot is the complete set of cluster objects needed for one arbitration.
type Snapshot struct {
	Now time.Time

	Cluster Limit
	// ClusterErr represents a present but unparsable cluster policy source.
	// Resolution fails closed unless a per-object last-known limit exists.
	ClusterErr error
	Teams      map[string]Limit

	Workloads []Workload

	// PreemptionGrace is the time a preemptor has to become node-ready before
	// its victims are restored. Zero uses DefaultPreemptionGrace.
	PreemptionGrace time.Duration
}

// ResourceShortfall describes unavailable capacity.
type ResourceShortfall struct {
	Nodes        int
	GPUMilli     int64
	Limited      bool
	Hard         bool
	LimitSource  Source
	EffectiveMax int
	ClusterUsed  int
	ClusterMax   int
	TeamUsed     int
	TeamMax      int
}

// Decision is the desired arbitration state for one Workspace.
type Decision struct {
	Key    string
	Phase  Phase
	Reason string

	Message string

	MissingNodes    int
	MissingGPUMilli int64
	LimitSource     Source
	EffectiveLimit  EffectiveLimit

	// ResolutionError is non-empty when a limit source was invalid but a
	// last-known effective value allowed arbitration to continue.
	ResolutionError string

	Shortfall ResourceShortfall

	PreemptKeys []string
	Preemption  *PreemptionState

	// Blocked means configuration resolution failed closed. The controller
	// should return/requeue rather than provision machines.
	Blocked bool

	// WaitSince is the value that should be persisted for queue timing.
	WaitSince time.Time

	// BackoffUntil asks the adapter to persist a preemption repetition block.
	BackoffUntil time.Time

	// RequeueAfter is a hint for the controller while waiting.
	RequeueAfter time.Duration
}

// Plan is a deterministic set of decisions.
type Plan struct {
	Decisions []Decision
}

// For returns the decision for key.
func (p Plan) For(key string) (Decision, bool) {
	for i := range p.Decisions {
		if p.Decisions[i].Key == key {
			return p.Decisions[i], true
		}
	}
	return Decision{}, false
}

// Ledger is the occupied quota. It is rebuilt from cluster objects on every
// arbitration; process caches never author it.
type Ledger struct {
	Nodes        int
	GPUMilli     int64
	WholeCards   int
	SliceMilli   int64
	TeamNodes    map[string]int
	TeamGPUMilli map[string]int64
}

// NewLedger returns an empty ledger.
func NewLedger() *Ledger {
	return &Ledger{TeamNodes: map[string]int{}, TeamGPUMilli: map[string]int64{}}
}

// Add reserves d for team. Callers are responsible for checking capacity.
func (l *Ledger) Add(team string, d Demand) {
	l.Nodes += maxInt(0, d.Nodes)
	l.GPUMilli += d.GPUMilli()
	l.WholeCards += maxInt(0, d.WholeGPUCards)
	l.SliceMilli += d.SliceMilliGPU()
	l.TeamNodes[team] += maxInt(0, d.Nodes)
	l.TeamGPUMilli[team] += d.GPUMilli()
}

// Remove releases d for team. It clamps at zero so a malformed/redundant event
// cannot produce negative occupancy; rebuilding remains the source of truth.
func (l *Ledger) Remove(team string, d Demand) {
	l.Nodes = maxInt(0, l.Nodes-maxInt(0, d.Nodes))
	l.GPUMilli = maxInt64(0, l.GPUMilli-d.GPUMilli())
	l.WholeCards = maxInt(0, l.WholeCards-maxInt(0, d.WholeGPUCards))
	l.SliceMilli = maxInt64(0, l.SliceMilli-d.SliceMilliGPU())
	l.TeamNodes[team] = maxInt(0, l.TeamNodes[team]-maxInt(0, d.Nodes))
	l.TeamGPUMilli[team] = maxInt64(0, l.TeamGPUMilli[team]-d.GPUMilli())
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// BuildLedger reconstructs occupancy from admitted workloads only.
func BuildLedger(workloads []Workload) *Ledger {
	l := NewLedger()
	for _, w := range workloads {
		if w.Failed || w.Deleting {
			continue
		}
		if w.Phase == PhaseAdmitted {
			l.Add(w.Team, w.Demand)
		}
	}
	return l
}

// Arbitrate computes a complete, deterministic plan. The same snapshot always
// produces the same plan.
func Arbitrate(in Snapshot) Plan {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	grace := in.PreemptionGrace
	if grace <= 0 {
		grace = 5 * time.Minute
	}

	workloads := append([]Workload(nil), in.Workloads...)

	decisions := map[string]Decision{}
	resolutionWarnings := map[string]string{}

	// Resolve limits and reject impossible/timed-out requests before queueing.
	effective := map[string]EffectiveLimit{}
	for i := range workloads {
		w := &workloads[i]
		clusterLayer := Layer{Source: SourceClusterDefault, Limit: in.Cluster, Err: in.ClusterErr}
		layers := append([]Layer{clusterLayer}, w.Layers...)
		if _, teamOK := in.Teams[w.Team]; teamOK {
			// Team layer is inserted after cluster and before workload layer.
			withTeam := []Layer{clusterLayer, {Source: SourceTeam, Limit: in.Teams[w.Team]}}
			withTeam = append(withTeam, w.Layers...)
			layers = withTeam
		}

		lim, err := Resolve(w.LastEffective, layers...)
		effective[w.Key] = lim
		if err != nil && w.LastEffective == nil {
			decisions[w.Key] = Decision{
				Key: w.Key, Phase: PhasePending, Reason: ReasonLimitResolutionFailed,
				Message:        err.Error(),
				Blocked:        true,
				EffectiveLimit: lim,
				LimitSource:    SourceNone,
				WaitSince:      waitSince(*w, now),
				RequeueAfter:   10 * time.Second,
			}
			continue
		}
		if err != nil {
			resolutionWarnings[w.Key] = err.Error()
		}

		if err := w.Demand.Validate(); err != nil {
			decisions[w.Key] = rejected(w.Key, ReasonInvalidDemand, err.Error(), lim)
			continue
		}

		if timedOut(*w, lim, now) {
			decisions[w.Key] = rejected(w.Key, ReasonWaitTimeout,
				"workload waited longer than its declared admission limit", lim)
			continue
		}
	}

	// Roll back failed/timed-out preemptions first. This restores victims and
	// removes the failed preemptor's occupancy before new admissions.
	for _, w := range workloads {
		if w.Preemption == nil || len(w.Preemption.Victims) == 0 {
			continue
		}
		if preemptionSucceeded(w) {
			continue
		}
		if !preemptionFailedOrExpired(w, now) {
			continue
		}

		attempt := w.Preemption.Attempt
		if attempt <= 0 {
			attempt = 1
		}
		blockedUntil := now.Add(preemptionBackoff(attempt))
		decisions[w.Key] = Decision{
			Key: w.Key, Phase: PhasePending, Reason: ReasonPreemptionRolledBack,
			Message:        "higher-priority workload did not obtain nodes within grace; restoring preempted workloads",
			EffectiveLimit: effective[w.Key],
			WaitSince:      waitSince(w, now),
			BackoffUntil:   blockedUntil,
			RequeueAfter:   preemptionBackoff(attempt + 1),
		}
		for _, victimKey := range w.Preemption.Victims {
			decisions[victimKey] = Decision{
				Key: victimKey, Phase: PhaseAdmitted, Reason: ReasonPreemptionRestored,
				Message:        "restored after failed higher-priority preemption",
				EffectiveLimit: effective[victimKey],
			}
		}
	}

	// Build the ledger from desired states after rollback.
	ledger := NewLedger()
	for _, w := range workloads {
		if w.Failed || w.Deleting {
			continue
		}
		d, decided := decisions[w.Key]
		phase := w.Phase
		if decided {
			phase = d.Phase
		}
		if phase == PhaseAdmitted {
			ledger.Add(w.Team, w.Demand)
		}
	}

	// Candidates preserve queue order. Rejected/blocked/rollback-pending items
	// are not scheduled in this pass.
	candidates := make([]Workload, 0, len(workloads))
	for _, w := range workloads {
		if _, ok := decisions[w.Key]; ok {
			continue
		}
		if w.Deleting {
			continue
		}
		if w.Phase == PhasePending || w.Phase == PhasePreempted || w.Phase == "" {
			candidates = append(candidates, w)
		}
	}
	sortCandidates(candidates, ledger, in.Teams, in.Cluster)

	for _, candidate := range candidates {
		lim := effective[candidate.Key]
		short := checkCapacity(candidate, lim, in.Cluster, in.Teams, ledger)
		if !short.Limited {
			admit(candidate, lim, nil, nil, decisions, ledger)
			continue
		}

		victims := selectVictims(candidate, short, workloads, decisions, now)
		if len(victims) > 0 {
			keys := make([]string, 0, len(victims))
			var state PreemptionState
			for _, v := range victims {
				keys = append(keys, v.Key)
				ledger.Remove(v.Team, v.Demand)
				attempt := 1
				if v.Preemption != nil {
					attempt = v.Preemption.Attempt
				}
				vState := &PreemptionState{
					Preemptor: candidate.Key,
					At:        now,
					Deadline:  now.Add(grace),
					Attempt:   attempt,
				}
				decisions[v.Key] = Decision{
					Key: v.Key, Phase: PhasePreempted, Reason: ReasonPreempted,
					Message:     fmt.Sprintf("preempted by higher-priority Workspace %s", candidate.Key),
					LimitSource: lim.NodeSource,
					Preemption:  vState,
					WaitSince:   waitSince(v, now),
				}
				state.Victims = append(state.Victims, v.Key)
			}
			state.At = now
			state.Deadline = now.Add(grace)
			state.Attempt = 1
			admit(candidate, lim, keys, &state, decisions, ledger)
			continue
		}

		// Head-of-line: once the ordered candidate cannot be admitted or
		// preempted, later candidates do not jump ahead.
		d := waitingDecision(candidate, lim, short, now)
		if candidate.Phase == PhasePreempted {
			d.Phase = PhasePreempted
			d.Reason = ReasonPreempted
			d.Preemption = candidate.Preemption
		}
		decisions[candidate.Key] = d
		break
	}

	// Every known workload gets an explicit stable decision when unchanged.
	out := make([]Decision, 0, len(workloads))
	for _, w := range workloads {
		if d, ok := decisions[w.Key]; ok {
			out = append(out, d)
			continue
		}
		if w.Phase == PhaseAdmitted {
			out = append(out, Decision{Key: w.Key, Phase: PhaseAdmitted, Reason: ReasonAdmitted, EffectiveLimit: effective[w.Key]})
			continue
		}
		// Defensive: unsorted non-candidate states remain pending.
		out = append(out, Decision{Key: w.Key, Phase: PhasePending, Reason: ReasonClusterQuotaWaiting, EffectiveLimit: effective[w.Key], WaitSince: waitSince(w, now)})
	}
	for i := range out {
		if warning, ok := resolutionWarnings[out[i].Key]; ok {
			out[i].ResolutionError = warning
		}
	}
	return Plan{Decisions: out}
}

func admit(w Workload, lim EffectiveLimit, keys []string, state *PreemptionState, decisions map[string]Decision, ledger *Ledger) {
	ledger.Add(w.Team, w.Demand)
	reason := ReasonAdmitted
	msg := "admitted to request nodes"
	if len(keys) > 0 {
		reason = ReasonPreemptionGranted
		msg = "admitted after preempting lower-priority Workspaces: " + strings.Join(keys, ",")
	}
	decisions[w.Key] = Decision{
		Key: w.Key, Phase: PhaseAdmitted, Reason: reason, Message: msg,
		EffectiveLimit: lim, PreemptKeys: keys, Preemption: state,
	}
}

func waitSince(w Workload, now time.Time) time.Time {
	if !w.WaitSince.IsZero() {
		return w.WaitSince
	}
	if !w.CreatedAt.IsZero() {
		return w.CreatedAt
	}
	return now
}

func timedOut(w Workload, lim EffectiveLimit, now time.Time) bool {
	if lim.MaxWait <= 0 {
		return false
	}
	since := w.WaitSince
	if since.IsZero() {
		since = w.CreatedAt
	}
	if since.IsZero() {
		return false
	}
	return now.Sub(since) >= lim.MaxWait
}

func rejected(key, reason, msg string, lim EffectiveLimit) Decision {
	return Decision{Key: key, Phase: PhaseRejected, Reason: reason, Message: msg, EffectiveLimit: lim}
}

func waitingDecision(w Workload, lim EffectiveLimit, short ResourceShortfall, now time.Time) Decision {
	var reason string
	switch short.LimitSource {
	case SourceTeam:
		reason = ReasonTeamQuotaWaiting
	case SourceWorkload:
		reason = ReasonDemandExceedsLimit
	default:
		reason = ReasonClusterQuotaWaiting
	}
	if short.Hard {
		if short.EffectiveMax == 0 {
			reason = ReasonQuotaLimitZero
		} else {
			reason = ReasonDemandExceedsLimit
		}
	}
	msg := fmt.Sprintf("waiting for %d more nodes", short.Nodes)
	if short.Hard {
		msg = fmt.Sprintf("demand exceeds effective limit by %d nodes", short.Nodes)
	}
	if short.LimitSource != SourceNone {
		msg += "; effective limit source=" + string(short.LimitSource)
	}
	return Decision{
		Key: w.Key, Phase: PhasePending, Reason: reason, Message: msg,
		MissingNodes: short.Nodes, MissingGPUMilli: short.GPUMilli,
		LimitSource: short.LimitSource, EffectiveLimit: lim, Shortfall: short,
		WaitSince: waitSince(w, now), RequeueAfter: 10 * time.Second,
	}
}

func checkCapacity(w Workload, lim EffectiveLimit, cluster Limit, teams map[string]Limit, l *Ledger) ResourceShortfall {
	short := ResourceShortfall{ClusterUsed: l.Nodes, TeamUsed: l.TeamNodes[w.Team]}

	if lim.NodesLimited() && w.Demand.Nodes > lim.MaxNodes {
		short.Limited = true
		short.Hard = true
		short.LimitSource = lim.NodeSource
		short.EffectiveMax = lim.MaxNodes
		short.Nodes = w.Demand.Nodes - lim.MaxNodes
	}
	if lim.GPULimited() && w.Demand.GPUMilli() > lim.MaxGPUMilli {
		short.Limited = true
		short.Hard = true
		if short.LimitSource == SourceNone {
			short.LimitSource = lim.GPUSource
		}
		short.GPUMilli = w.Demand.GPUMilli() - lim.MaxGPUMilli
	}

	if cluster.MaxNodes != nil && *cluster.MaxNodes >= 0 {
		short.ClusterMax = *cluster.MaxNodes
		if used, need := l.Nodes+w.Demand.Nodes, *cluster.MaxNodes; used > need {
			short.Limited = true
			short.Nodes = maxInt(short.Nodes, used-need)
			if !short.Hard {
				short.LimitSource = SourceClusterDefault
				short.EffectiveMax = need
			}
		}
	}
	if team, ok := teams[w.Team]; ok && team.MaxNodes != nil && *team.MaxNodes >= 0 {
		max := *team.MaxNodes
		short.TeamMax = max
		if used := l.TeamNodes[w.Team] + w.Demand.Nodes; used > max {
			short.Limited = true
			short.Nodes = maxInt(short.Nodes, used-max)
			if !short.Hard || short.LimitSource != SourceWorkload {
				// Team source is more specific than cluster source when both bind.
				short.LimitSource = SourceTeam
				short.EffectiveMax = max
			}
		}
	}
	if lim.GPULimited() {
		if used := l.GPUMilli + w.Demand.GPUMilli(); used > lim.MaxGPUMilli {
			short.Limited = true
			if short.LimitSource == SourceNone {
				short.LimitSource = lim.GPUSource
			}
			short.GPUMilli = used - lim.MaxGPUMilli
		}
	}
	return short
}

func sortCandidates(ws []Workload, l *Ledger, teams map[string]Limit, cluster Limit) {
	sort.SliceStable(ws, func(i, j int) bool {
		a, b := ws[i], ws[j]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}

		ai, bi := l.TeamNodes[a.Team], l.TeamNodes[b.Team]
		ad := fairDenominator(a.Team, teams, cluster)
		bd := fairDenominator(b.Team, teams, cluster)
		if c := compareRatio(ai, ad, bi, bd); c != 0 {
			return c < 0
		}

		if !a.CreatedAt.Equal(b.CreatedAt) {
			if a.CreatedAt.IsZero() != b.CreatedAt.IsZero() {
				return !a.CreatedAt.IsZero()
			}
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.Key < b.Key
	})
}

func fairDenominator(team string, teams map[string]Limit, cluster Limit) int {
	if t, ok := teams[team]; ok && t.MaxNodes != nil && *t.MaxNodes > 0 {
		return *t.MaxNodes
	}
	if cluster.MaxNodes != nil && *cluster.MaxNodes > 0 {
		return *cluster.MaxNodes
	}
	return 0 // unlimited => ratio zero
}

func compareRatio(aUsed, aDen int, bUsed, bDen int) int {
	if aDen <= 0 && bDen <= 0 {
		return 0
	}
	if aDen <= 0 {
		return -1
	}
	if bDen <= 0 {
		return 1
	}
	left := int64(aUsed) * int64(bDen)
	right := int64(bUsed) * int64(aDen)
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func selectVictims(candidate Workload, short ResourceShortfall, all []Workload, decided map[string]Decision, now time.Time) []Workload {
	if !candidate.PreemptionBlockedUntil.IsZero() && now.Before(candidate.PreemptionBlockedUntil) {
		return nil
	}

	eligible := make([]Workload, 0)
	for _, w := range all {
		if w.Key == candidate.Key || w.Priority >= candidate.Priority {
			continue // same priority never preempts; lower only.
		}
		if d, ok := decided[w.Key]; ok && d.Phase != PhaseAdmitted {
			continue
		}
		if w.Phase != PhaseAdmitted {
			continue
		}
		if w.Preemption != nil && len(w.Preemption.Victims) > 0 && !preemptionFailedOrExpired(w, now) {
			continue
		}
		if !w.Ready && !w.Yieldable {
			// Not-ready capacity is preemptible by definition. Yieldable is
			// only needed to preempt live/serving capacity.
		}
		if w.Ready && !w.Yieldable {
			continue
		}
		eligible = append(eligible, w)
	}

	needNodes := maxInt(short.Nodes, 0)
	needGPU := short.GPUMilli
	if needNodes == 0 && needGPU == 0 {
		// Capacity check can bind only on one dimension. Use candidate demand
		// conservatively if shortfall was not populated by an older caller.
		needNodes = candidate.Demand.Nodes
		needGPU = candidate.Demand.GPUMilli()
	}

	// A single sufficient victim is always preferred, choosing the smallest
	// sufficient release to give away only just enough.
	var single *Workload
	for i := range eligible {
		v := &eligible[i]
		if v.Demand.Nodes >= needNodes && v.Demand.GPUMilli() >= needGPU {
			if single == nil || releaseScore(v.Demand) < releaseScore(single.Demand) {
				single = v
			}
		}
	}
	if single != nil {
		return []Workload{*single}
	}

	// Find the minimum-cardinality set. Quota populations per pool are small;
	// keep the search deterministic and bounded by the eligible set.
	sort.SliceStable(eligible, func(i, j int) bool {
		// Largest releasers first makes the cardinality search find a bound
		// quickly; tie by stable key.
		if eligible[i].Demand.Nodes != eligible[j].Demand.Nodes {
			return eligible[i].Demand.Nodes > eligible[j].Demand.Nodes
		}
		if eligible[i].Demand.GPUMilli() != eligible[j].Demand.GPUMilli() {
			return eligible[i].Demand.GPUMilli() > eligible[j].Demand.GPUMilli()
		}
		return eligible[i].Key < eligible[j].Key
	})

	for k := 2; k <= len(eligible); k++ {
		if set, ok := minimumSet(eligible, k, needNodes, needGPU); ok {
			return set
		}
	}
	return nil
}

func releaseScore(d Demand) int64 {
	return int64(d.Nodes)*MilliGPUPerCard + d.GPUMilli()
}

func minimumSet(ws []Workload, k, needNodes int, needGPU int64) ([]Workload, bool) {
	type combo struct {
		idx []int
	}
	var best []Workload
	bestExcess := int64(1<<62 - 1)
	var visit func(start int, chosen []int)
	visit = func(start int, chosen []int) {
		if len(chosen) == k {
			var nodes int
			var gpu int64
			out := make([]Workload, 0, k)
			for _, idx := range chosen {
				nodes += ws[idx].Demand.Nodes
				gpu += ws[idx].Demand.GPUMilli()
				out = append(out, ws[idx])
			}
			if nodes >= needNodes && gpu >= needGPU {
				excess := int64(nodes-needNodes)*MilliGPUPerCard + (gpu - needGPU)
				if excess < bestExcess {
					bestExcess = excess
					best = out
				}
			}
			return
		}
		for i := start; i <= len(ws)-(k-len(chosen)); i++ {
			visit(i+1, append(chosen, i))
		}
	}
	visit(0, nil)
	if best == nil {
		return nil, false
	}
	return best, true
}

func preemptionSucceeded(w Workload) bool {
	return w.NodeReady || (w.Ready && w.Preemption != nil && len(w.Preemption.Victims) > 0)
}

func preemptionFailedOrExpired(w Workload, now time.Time) bool {
	if w.Preemption == nil {
		return false
	}
	// A terminal/failed target is represented by Ready=false and an explicit
	// expired deadline. Adapters can set Deadline to now on observed failure.
	deadline := w.Preemption.Deadline
	if deadline.IsZero() {
		deadline = w.Preemption.At.Add(5 * time.Minute)
	}
	return !w.NodeReady && !deadline.After(now)
}

func preemptionBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(1<<minInt(attempt-1, 5)) * time.Minute
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
