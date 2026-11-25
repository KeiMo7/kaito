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

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsRejectionIncrementsOnlyOnTransition(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	decision := Decision{Phase: PhaseRejected, Reason: ReasonWaitTimeout}

	m.RecordRejectionTransition("team-a", decision, PhasePending)
	m.RecordRejectionTransition("team-a", decision, PhaseRejected)
	m.RecordRejectionTransition("team-a", decision, PhaseRejected)

	counter, err := m.Rejected.GetMetricWithLabelValues("team-a", ReasonWaitTimeout)
	require.NoError(t, err)
	pb := &dto.Metric{}
	require.NoError(t, counter.Write(pb))
	assert.Equal(t, float64(1), pb.Counter.GetValue(), "repeated reconciles must not accumulate rejections")
}

func TestMetricsWaitingGaugeIsReconciledNotAccumulated(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	counts := map[string]map[string]int{"team-a": {ReasonTeamQuotaWaiting: 2}}

	m.SetWaiting(counts)
	m.SetWaiting(counts)

	gauge, err := m.Waiting.GetMetricWithLabelValues("team-a", ReasonTeamQuotaWaiting)
	require.NoError(t, err)
	pb := &dto.Metric{}
	require.NoError(t, gauge.Write(pb))
	assert.Equal(t, float64(2), pb.Gauge.GetValue())
}
