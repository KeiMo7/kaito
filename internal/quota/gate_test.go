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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
)

func gateTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, kaitov1beta1.AddToScheme(s))
	return s
}

func quotaWorkspace(name, team string, target int32, created time.Time, state Phase) *kaitov1beta1.Workspace {
	rawState, _ := EncodeState(PersistedState{Phase: state})
	ws := &kaitov1beta1.Workspace{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "team-ns",
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
			Annotations: map[string]string{
				AnnotationTeam:  team,
				AnnotationState: rawState,
			},
		},
		Status: kaitov1beta1.WorkspaceStatus{TargetNodeCount: target},
	}
	if state == PhaseAdmitted {
		ws.Status.State = kaitov1beta1.WorkspaceStateReady
	}
	return ws
}

func TestGateFakeClientContentionScaleDownAndRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 25, 6, 0, 0, 0, time.UTC)
	policyCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-ns", Name: ConfigMapName},
		Data:       map[string]string{ConfigMapKey: `{"defaultLimit":{"maxNodes":2}}`},
	}
	alpha := quotaWorkspace("alpha", "a", 2, now.Add(-2*time.Minute), PhaseAdmitted)
	beta := quotaWorkspace("beta", "b", 1, now.Add(-time.Minute), PhasePending)

	cl := fake.NewClientBuilder().
		WithScheme(gateTestScheme(t)).
		WithObjects(policyCM, alpha, beta).
		WithStatusSubresource(&kaitov1beta1.Workspace{}).
		Build()

	newGate := func() *Gate {
		g := NewGate(cl, "team-ns", nil, nil)
		g.Now = func() time.Time { return now }
		g.Metrics = NewMetrics(prometheus.NewRegistry())
		return g
	}

	d, err := newGate().Admit(ctx, beta)
	require.NoError(t, err)
	require.NotNil(t, d)
	assert.Equal(t, PhasePending, d.Phase)
	assert.Equal(t, 1, d.MissingNodes)

	// Alpha scales down. A new Gate instance has no in-memory queue or cache;
	// state and order are rebuilt from the same fake cluster objects.
	var alphaLatest kaitov1beta1.Workspace
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(alpha), &alphaLatest))
	alphaLatest.Status.TargetNodeCount = 1
	require.NoError(t, cl.Status().Update(ctx, &alphaLatest))

	d, err = newGate().Admit(ctx, beta)
	require.NoError(t, err)
	require.NotNil(t, d)
	assert.Equal(t, PhaseAdmitted, d.Phase, d.Message)

	var betaLatest kaitov1beta1.Workspace
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(beta), &betaLatest))
	state, err := DecodeState(betaLatest.Annotations)
	require.NoError(t, err)
	assert.Equal(t, PhaseAdmitted, state.Phase)
}
