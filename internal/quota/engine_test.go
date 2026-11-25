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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intPtr(v int) *int { return &v }

func TestResolveTightestLimitAndInvalidSource(t *testing.T) {
	cluster := Layer{Source: SourceClusterDefault, Limit: Limit{MaxNodes: intPtr(10)}}
	team := Layer{Source: SourceTeam, Limit: Limit{MaxNodes: intPtr(4)}}
	workload := Layer{Source: SourceWorkload, Limit: Limit{MaxNodes: intPtr(8)}}

	got, err := Resolve(nil, cluster, team, workload)
	require.NoError(t, err)
	assert.Equal(t, 4, got.MaxNodes)
	assert.Equal(t, SourceTeam, got.NodeSource)

	// Unset means unlimited; no layers at all preserves today's behavior.
	got, err = Resolve(nil)
	require.NoError(t, err)
	assert.True(t, got.MaxNodes < 0)

	last := Unlimited()
	last.MaxNodes = 3
	last.NodeSource = SourceTeam
	badTeam := Layer{Source: SourceTeam, Err: assert.AnError}
	got, err = Resolve(&last, cluster, badTeam, workload)
	require.Error(t, err)
	assert.True(t, got.Stale)
	assert.Equal(t, 3, got.MaxNodes, "invalid source must retain last effective value")

	_, err = Resolve(nil, badTeam)
	require.Error(t, err, "no last known value must fail closed")
}

func TestDemandConversionAndCardOverlap(t *testing.T) {
	d := Demand{
		Nodes:         2,
		WholeGPUCards: 1,
		Slices: []SliceDemand{
			{Count: 2, Granularity: 2}, // half-card slices
			{Count: 1, Granularity: 4}, // quarter-card slice
		},
	}
	require.NoError(t, d.Validate())
	assert.Equal(t, int64(2250), d.GPUMilli()) // 1000 + 2*500 + 250

	ledger := NewUniformCardLedger(1, 4)
	require.True(t, ledger.AllocateWhole())
	assert.False(t, ledger.AllocateWhole(), "two whole cards cannot overlap one physical card")
	assert.False(t, ledger.AllocateSlice(1), "a whole card cannot also host slices")
	assert.True(t, ledger.Cards[0].Whole)
	assert.Equal(t, 4, ledger.Cards[0].UsedSlots)

	mixed := NewUniformCardLedger(2, 2)
	require.True(t, mixed.AllocateDemand(Demand{WholeGPUCards: 1}))
	assert.True(t, mixed.CanAllocateDemand(Demand{Slices: []SliceDemand{{Count: 2, SlotsPerSlice: 1}}}))
	assert.False(t, mixed.CanAllocateDemand(Demand{WholeGPUCards: 2}), "only one unused card remains")
}

func TestTwoTeamsContentionThenScaleDownAdmitsQueuedTeam(t *testing.T) {
	now := time.Date(2026, 11, 25, 4, 0, 0, 0, time.UTC)
	cluster := Limit{MaxNodes: intPtr(2)}
	base := Snapshot{Now: now, Cluster: cluster}

	alpha := Workload{
		Key: "team-a/alpha", Team: "a", Priority: 10, CreatedAt: now.Add(-2 * time.Minute),
		Demand: Demand{Nodes: 2}, Phase: PhaseAdmitted,
	}
	beta := Workload{
		Key: "team-b/beta", Team: "b", Priority: 10, CreatedAt: now.Add(-1 * time.Minute),
		Demand: Demand{Nodes: 1}, Phase: PhasePending, WaitSince: now.Add(-time.Minute),
	}

	plan := Arbitrate(func() Snapshot {
		s := base
		s.Workloads = []Workload{alpha, beta}
		return s
	}())
	betaDecision, ok := plan.For(beta.Key)
	require.True(t, ok)
	assert.Equal(t, PhasePending, betaDecision.Phase)
	assert.Equal(t, ReasonClusterQuotaWaiting, betaDecision.Reason)
	assert.Equal(t, 1, betaDecision.MissingNodes)
	assert.Equal(t, SourceClusterDefault, betaDecision.LimitSource)

	// Alpha scales from two nodes to one; rebuilding occupancy from its current
	// object demand releases one node and beta is admitted without resubmission.
	scaledAlpha := alpha
	scaledAlpha.Demand.Nodes = 1
	plan = Arbitrate(func() Snapshot {
		s := base
		s.Workloads = []Workload{scaledAlpha, beta}
		return s
	}())
	betaDecision, ok = plan.For(beta.Key)
	require.True(t, ok)
	assert.Equal(t, PhaseAdmitted, betaDecision.Phase, betaDecision.Message)
	alphaDecision, _ := plan.For(alpha.Key)
	assert.Equal(t, PhaseAdmitted, alphaDecision.Phase)
}

func TestFairnessOrderRatioSubmissionAndName(t *testing.T) {
	now := time.Now()
	cluster := Limit{MaxNodes: intPtr(2)}
	teams := map[string]Limit{
		"a": {MaxNodes: intPtr(2)},
		"b": {MaxNodes: intPtr(2)},
	}

	// A already uses one of its two nodes; B uses none. B must go first even
	// though A's candidate appears first and has an earlier creation time.
	usedA := Workload{Key: "a/used", Team: "a", Priority: 5, Ready: true, Demand: Demand{Nodes: 1}, Phase: PhaseAdmitted}
	usedB := Workload{Key: "b/used", Team: "b", Priority: 5, Ready: true, Demand: Demand{Nodes: 0}, Phase: PhaseAdmitted}
	candidateA := Workload{Key: "a/candidate", Team: "a", Priority: 5, CreatedAt: now.Add(-time.Hour), Demand: Demand{Nodes: 1}, Phase: PhasePending}
	candidateB := Workload{Key: "b/candidate", Team: "b", Priority: 5, CreatedAt: now.Add(-time.Minute), Demand: Demand{Nodes: 1}, Phase: PhasePending}

	s := Snapshot{Now: now, Cluster: cluster, Teams: teams, Workloads: []Workload{candidateA, candidateB, usedA, usedB}}
	plan := Arbitrate(s)
	db, _ := plan.For(candidateB.Key)
	da, _ := plan.For(candidateA.Key)
	assert.Equal(t, PhaseAdmitted, db.Phase)
	assert.Equal(t, PhasePending, da.Phase, "only one shared-pool node remains after B is admitted")

	// Equal ratios, equal priority: earlier submission wins.
	earlier := Workload{Key: "team/x-earlier", Team: "x", Priority: 1, CreatedAt: now.Add(-2 * time.Minute), Demand: Demand{Nodes: 1}, Phase: PhasePending}
	later := Workload{Key: "team/x-later", Team: "x", Priority: 1, CreatedAt: now.Add(-time.Minute), Demand: Demand{Nodes: 1}, Phase: PhasePending}
	l := NewLedger()
	sortCandidates([]Workload{later, earlier}, l, teams, cluster)
	ordered := []Workload{later, earlier}
	sortCandidates(ordered, l, teams, cluster)
	assert.Equal(t, "team/x-earlier", ordered[0].Key)

	// Same submission time: namespaced name gives a stable order.
	z := Workload{Key: "team/z", Priority: 1, CreatedAt: now, Demand: Demand{Nodes: 1}}
	a := Workload{Key: "team/a", Priority: 1, CreatedAt: now, Demand: Demand{Nodes: 1}}
	ordered = []Workload{z, a}
	sortCandidates(ordered, l, teams, cluster)
	assert.Equal(t, "team/a", ordered[0].Key)
}

func TestPriorityPreemptionChoosesOnlyJustEnoughVictim(t *testing.T) {
	now := time.Now()
	cluster := Limit{MaxNodes: intPtr(3)}
	small := Workload{Key: "low/small", Priority: 1, Team: "t", Demand: Demand{Nodes: 1}, Phase: PhaseAdmitted}
	big := Workload{Key: "low/big", Priority: 1, Team: "t", Demand: Demand{Nodes: 2}, Phase: PhaseAdmitted}
	high := Workload{Key: "high/main", Priority: 10, Team: "t", Demand: Demand{Nodes: 2}, Phase: PhasePending, CreatedAt: now}

	plan := Arbitrate(Snapshot{Now: now, Cluster: cluster, Workloads: []Workload{small, big, high}})
	hd, ok := plan.For(high.Key)
	require.True(t, ok)
	assert.Equal(t, PhaseAdmitted, hd.Phase)
	assert.Equal(t, []string{"low/big"}, hd.PreemptKeys, "one sufficient victim must be chosen")
	bd, _ := plan.For(big.Key)
	assert.Equal(t, PhasePreempted, bd.Phase)
	sd, _ := plan.For(small.Key)
	assert.Equal(t, PhaseAdmitted, sd.Phase, "unnecessary lower-priority workload must stay admitted")
}

func TestSamePriorityCannotPreempt(t *testing.T) {
	now := time.Now()
	cluster := Limit{MaxNodes: intPtr(1)}
	low := Workload{Key: "same/one", Priority: 5, Demand: Demand{Nodes: 1}, Phase: PhaseAdmitted}
	other := Workload{Key: "same/two", Priority: 5, Demand: Demand{Nodes: 1}, Phase: PhasePending}
	plan := Arbitrate(Snapshot{Now: now, Cluster: cluster, Workloads: []Workload{low, other}})
	d, _ := plan.For(other.Key)
	assert.Equal(t, PhasePending, d.Phase)
	assert.Empty(t, d.PreemptKeys)
}

func TestPreemptionRollbackRestoresVictimAndDoesNotRepeat(t *testing.T) {
	now := time.Date(2026, 11, 25, 5, 0, 0, 0, time.UTC)
	deadline := now.Add(-time.Minute)
	victim := Workload{
		Key: "low/victim", Priority: 1, Demand: Demand{Nodes: 1}, Phase: PhasePreempted,
		Preemption: &PreemptionState{Preemptor: "high/main", At: now.Add(-10 * time.Minute), Deadline: deadline},
	}
	high := Workload{
		Key: "high/main", Priority: 10, Demand: Demand{Nodes: 2}, Phase: PhaseAdmitted,
		Preemption: &PreemptionState{Victims: []string{victim.Key}, At: now.Add(-10 * time.Minute), Deadline: deadline, Attempt: 1},
	}
	cluster := Limit{MaxNodes: intPtr(2)}

	plan := Arbitrate(Snapshot{Now: now, Cluster: cluster, Workloads: []Workload{victim, high}})
	hd, _ := plan.For(high.Key)
	assert.Equal(t, PhasePending, hd.Phase)
	assert.Equal(t, ReasonPreemptionRolledBack, hd.Reason)
	assert.False(t, hd.BackoffUntil.IsZero())
	vd, _ := plan.For(victim.Key)
	assert.Equal(t, PhaseAdmitted, vd.Phase, "victim must be restored when high priority misses grace")

	// Persisted backoff is now respected: the same victim is not preempted again.
	high.Phase = PhasePending
	high.Preemption = nil
	high.PreemptionBlockedUntil = hd.BackoffUntil.Add(time.Minute)
	victim.Phase = PhaseAdmitted
	victim.Preemption = nil
	plan = Arbitrate(Snapshot{Now: now.Add(time.Second), Cluster: cluster, Workloads: []Workload{victim, high}})
	hd, _ = plan.For(high.Key)
	assert.Equal(t, PhasePending, hd.Phase)
	assert.Empty(t, hd.PreemptKeys)
}

func TestZeroLimitAndDemandAboveTeamLimitWaitInsteadOfAdmit(t *testing.T) {
	now := time.Now()
	w := Workload{Key: "t/w", Team: "t", Demand: Demand{Nodes: 1}, Phase: PhasePending}

	plan := Arbitrate(Snapshot{
		Now: now, Cluster: Limit{MaxNodes: intPtr(0)},
		Workloads: []Workload{w},
	})
	d, _ := plan.For(w.Key)
	assert.Equal(t, PhasePending, d.Phase, "hard cap remains queued until the declared wait bound")
	assert.Equal(t, ReasonQuotaLimitZero, d.Reason)
	assert.True(t, d.Shortfall.Hard)
	assert.Equal(t, SourceClusterDefault, d.LimitSource)

	teamOnly := Snapshot{
		Now:   now,
		Teams: map[string]Limit{"t": {MaxNodes: intPtr(1)}},
		Workloads: []Workload{{
			Key: "t/big", Team: "t", Demand: Demand{Nodes: 2}, Phase: PhasePending,
		}},
	}
	plan = Arbitrate(teamOnly)
	d, _ = plan.For("t/big")
	assert.Equal(t, PhasePending, d.Phase)
	assert.Equal(t, ReasonDemandExceedsLimit, d.Reason)
	assert.Equal(t, SourceTeam, d.LimitSource)
	assert.True(t, d.Shortfall.Hard)
	assert.Equal(t, 1, d.MissingNodes)
}

func TestHardLimitWaitTimeoutIsRejected(t *testing.T) {
	now := time.Now()
	w := Workload{
		Key: "t/big", Team: "t", Demand: Demand{Nodes: 2}, Phase: PhasePending,
		WaitSince: now.Add(-2 * time.Hour),
		Layers: []Layer{{Source: SourceTeam, Limit: Limit{
			MaxNodes: intPtr(1), MaxWait: time.Hour,
		}}},
	}
	plan := Arbitrate(Snapshot{Now: now, Workloads: []Workload{w}})
	d, _ := plan.For(w.Key)
	assert.Equal(t, PhaseRejected, d.Phase)
	assert.Equal(t, ReasonWaitTimeout, d.Reason)
}

func TestWaitTimeoutIsRejectedAndLeavesNoWaitingDecision(t *testing.T) {
	now := time.Now()
	maxWait := time.Hour
	w := Workload{
		Key: "t/w", Team: "t", Demand: Demand{Nodes: 5}, Phase: PhasePending,
		WaitSince: now.Add(-2 * time.Hour),
		Layers:    []Layer{{Source: SourceWorkload, Limit: Limit{MaxWait: maxWait, MaxNodes: intPtr(10)}}},
	}
	plan := Arbitrate(Snapshot{Now: now, Workloads: []Workload{w}})
	d, _ := plan.For(w.Key)
	assert.Equal(t, PhaseRejected, d.Phase)
	assert.Equal(t, ReasonWaitTimeout, d.Reason)
}

func TestLedgerReleasesDeletedScaledAndFailedOccupancy(t *testing.T) {
	ws := []Workload{
		{Key: "a/one", Team: "a", Demand: Demand{Nodes: 2}, Phase: PhaseAdmitted},
		{Key: "a/two", Team: "a", Demand: Demand{Nodes: 1}, Phase: PhaseAdmitted, Failed: true},
		{Key: "a/three", Team: "a", Demand: Demand{Nodes: 1}, Phase: PhaseAdmitted, Deleting: true},
	}
	l := BuildLedger(ws)
	assert.Equal(t, 2, l.Nodes, "failed and deleting workloads do not occupy quota")

	// Scale the remaining object to one.
	ws[0].Demand.Nodes = 1
	l = BuildLedger(ws)
	assert.Equal(t, 1, l.Nodes)

	// Deletion is represented by absence from the rebuilt object list.
	l = BuildLedger(nil)
	assert.Equal(t, 0, l.Nodes)
	assert.Equal(t, 0, l.TeamNodes["a"], "no ghost team occupancy remains")
}

func TestArbitrateAllowsExistingAdmittedWorkloadDuringSurgeOverage(t *testing.T) {
	now := time.Now()
	cluster := Limit{MaxNodes: intPtr(2)}
	// Rolling update temporarily makes actual/desired object demand exceed the
	// steady cap. The arbiter must not evict or reject the existing occupant;
	// it only blocks new admissions.
	surge := Workload{Key: "a/rolling", Team: "a", Demand: Demand{Nodes: 3}, Phase: PhaseAdmitted, Ready: true}
	fresh := Workload{Key: "b/new", Team: "b", Priority: 1, Demand: Demand{Nodes: 1}, Phase: PhasePending}
	plan := Arbitrate(Snapshot{Now: now, Cluster: cluster, Workloads: []Workload{surge, fresh}})
	sd, _ := plan.For(surge.Key)
	fd, _ := plan.For(fresh.Key)
	assert.Equal(t, PhaseAdmitted, sd.Phase)
	assert.Equal(t, PhasePending, fd.Phase)
}

func TestInvalidClusterLimitSourceKeepsLastGoodOrFailsClosed(t *testing.T) {
	now := time.Now()
	last := Unlimited()
	last.MaxNodes = 1
	last.NodeSource = SourceTeam

	withLast := Workload{
		Key: "t/a", Team: "t", Demand: Demand{Nodes: 1}, Phase: PhasePending,
		LastEffective: &last,
	}
	plan := Arbitrate(Snapshot{Now: now, ClusterErr: assert.AnError, Workloads: []Workload{withLast}})
	d, _ := plan.For("t/a")
	assert.NotEmpty(t, d.ResolutionError)
	assert.Equal(t, 1, d.EffectiveLimit.MaxNodes)
	assert.Equal(t, PhaseAdmitted, d.Phase, "last known good limit remains usable")

	withoutLast := Workload{Key: "t/b", Team: "t", Demand: Demand{Nodes: 1}, Phase: PhasePending}
	plan = Arbitrate(Snapshot{Now: now, ClusterErr: assert.AnError, Workloads: []Workload{withoutLast}})
	d, _ = plan.For("t/b")
	assert.True(t, d.Blocked)
	assert.Equal(t, ReasonLimitResolutionFailed, d.Reason)
}

func TestArbitrateIsIdempotentForSameSnapshot(t *testing.T) {
	now := time.Now()
	s := Snapshot{
		Now: now, Cluster: Limit{MaxNodes: intPtr(2)},
		Teams: map[string]Limit{"t": {MaxNodes: intPtr(3)}},
		Workloads: []Workload{
			{Key: "t/a", Team: "t", Priority: 2, CreatedAt: now, Demand: Demand{Nodes: 1}, Phase: PhaseAdmitted},
			{Key: "t/b", Team: "t", Priority: 1, CreatedAt: now, Demand: Demand{Nodes: 2}, Phase: PhasePending},
		},
	}
	first := Arbitrate(s)
	second := Arbitrate(s)
	assert.Equal(t, first, second)
}
