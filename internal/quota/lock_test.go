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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestConfigMapLockMutualExclusion(t *testing.T) {
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	now := time.Date(2026, 11, 25, 7, 0, 0, 0, time.UTC)

	first := Lock{Client: cl, Namespace: "kaito-system", TTL: time.Minute, Now: func() time.Time { return now }}
	releaseFirst, err := first.Acquire(context.Background())
	require.NoError(t, err)

	second := Lock{Client: cl, Namespace: "kaito-system", TTL: time.Minute, Now: func() time.Time { return now }}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, err = second.Acquire(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "second arbiter must not enter while lock is held")

	require.NoError(t, releaseFirst(context.Background()))

	third := Lock{Client: cl, Namespace: "kaito-system", TTL: time.Minute, Now: func() time.Time { return now.Add(time.Second) }}
	releaseThird, err := third.Acquire(context.Background())
	require.NoError(t, err)
	assert.NoError(t, releaseThird(context.Background()))
}
