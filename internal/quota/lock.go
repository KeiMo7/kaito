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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	lockConfigMapName = "kaito-quota-arbitration-lock"
	lockOwnerKey      = AnnotationPrefix + "lock-owner"
	lockExpiryKey     = AnnotationPrefix + "lock-expiry"
)

// Lock is a short-lived cluster-object mutex. It makes two controller instances
// serialize their list -> arbitrate -> compare-and-patch transactions without
// introducing a new component.
type Lock struct {
	Client    client.Client
	Namespace string
	TTL       time.Duration
	Now       func() time.Time
}

func (l Lock) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

// Acquire waits until the lock is acquired or ctx ends. The returned release
// function only removes a lock still owned by this caller.
func (l Lock) Acquire(ctx context.Context) (func(context.Context) error, error) {
	if l.TTL <= 0 {
		l.TTL = 30 * time.Second
	}
	owner, err := randomOwner()
	if err != nil {
		return nil, err
	}
	key := client.ObjectKey{Namespace: l.Namespace, Name: lockConfigMapName}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cm := &corev1.ConfigMap{}
		err := l.Client.Get(ctx, key, cm)
		switch {
		case apierrors.IsNotFound(err):
			cm = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: key.Namespace,
					Name:      key.Name,
					Annotations: map[string]string{
						lockOwnerKey:  owner,
						lockExpiryKey: l.now().Add(l.TTL).Format(time.RFC3339Nano),
					},
				},
			}
			if createErr := l.Client.Create(ctx, cm); createErr != nil {
				if apierrors.IsAlreadyExists(createErr) {
					continue
				}
				return nil, createErr
			}
			return l.release(owner), nil
		case err != nil:
			return nil, err
		}

		acquired, patchErr := l.tryTake(ctx, cm, owner)
		if patchErr != nil {
			if apierrors.IsConflict(patchErr) {
				continue
			}
			return nil, patchErr
		}
		if acquired {
			return l.release(owner), nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (l Lock) tryTake(ctx context.Context, cm *corev1.ConfigMap, owner string) (bool, error) {
	currentOwner := cm.Annotations[lockOwnerKey]
	expired := true
	if raw := cm.Annotations[lockExpiryKey]; raw != "" {
		if expiry, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			expired = !l.now().Before(expiry)
		}
	}
	if currentOwner != "" && currentOwner != owner && !expired {
		return false, nil
	}

	patch := client.MergeFromWithOptions(cm.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if cm.Annotations == nil {
		cm.Annotations = map[string]string{}
	}
	cm.Annotations[lockOwnerKey] = owner
	cm.Annotations[lockExpiryKey] = l.now().Add(l.TTL).Format(time.RFC3339Nano)
	if err := l.Client.Patch(ctx, cm, patch); err != nil {
		return false, err
	}
	return true, nil
}

func (l Lock) release(owner string) func(context.Context) error {
	return func(ctx context.Context) error {
		key := client.ObjectKey{Namespace: l.Namespace, Name: lockConfigMapName}
		return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			cm := &corev1.ConfigMap{}
			if err := l.Client.Get(ctx, key, cm); err != nil {
				return client.IgnoreNotFound(err)
			}
			if cm.Annotations[lockOwnerKey] != owner {
				return nil
			}
			patch := client.MergeFromWithOptions(cm.DeepCopy(), client.MergeFromWithOptimisticLock{})
			delete(cm.Annotations, lockOwnerKey)
			delete(cm.Annotations, lockExpiryKey)
			return l.Client.Patch(ctx, cm, patch)
		})
	}
}

func randomOwner() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate lock owner: %w", err)
	}
	return hex.EncodeToString(b), nil
}
