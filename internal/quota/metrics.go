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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metrics holds quota observability. Waiting is a gauge reconciled from state;
// rejections are incremented only on a Pending/Admitted -> Rejected transition,
// so repeated reconciles do not accumulate.
type Metrics struct {
	Waiting  *prometheus.GaugeVec
	Rejected *prometheus.CounterVec
}

// NewMetrics creates metrics and registers them with reg when non-nil.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Waiting: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kaito_quota_waiting",
			Help: "Number of Workspaces currently waiting for in-cluster quota, by team and reason.",
		}, []string{"team", "reason"}),
		Rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kaito_quota_rejected_total",
			Help: "Number of Workspace admissions rejected by in-cluster quota, by team and reason.",
		}, []string{"team", "reason"}),
	}
	if reg != nil {
		reg.MustRegister(m.Waiting, m.Rejected)
	}
	return m
}

var defaultMetrics = NewMetrics(metrics.Registry)

// DefaultMetrics returns the metrics registered with controller-runtime.
func DefaultMetrics() *Metrics { return defaultMetrics }

// SetWaiting replaces the current waiting series from a reconciled snapshot.
// Counts is keyed by team then stable reason.
func (m *Metrics) SetWaiting(counts map[string]map[string]int) {
	m.Waiting.Reset()
	for team, reasons := range counts {
		for reason, count := range reasons {
			m.Waiting.WithLabelValues(team, reason).Set(float64(count))
		}
	}
}

// RecordRejectionTransition increments the rejection counter only when the
// object was not already Rejected.
func (m *Metrics) RecordRejectionTransition(team string, d Decision, previous Phase) {
	if d.Phase != PhaseRejected || previous == PhaseRejected {
		return
	}
	m.Rejected.WithLabelValues(team, d.Reason).Inc()
}
