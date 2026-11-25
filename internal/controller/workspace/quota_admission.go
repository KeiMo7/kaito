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

package workspace

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/internal/quota"
)

const quotaWaitRequeue = 10 * time.Second

// admitByQuota runs the unified gate after node demand has been estimated and
// before ProvisionNodes can request machines. A nil gate or an absent quota
// configuration preserves the legacy path.
func (c *WorkspaceReconciler) admitByQuota(ctx context.Context, ws *kaitov1beta1.Workspace) (bool, reconcile.Result, error) {
	if c.QuotaGate == nil {
		return true, reconcile.Result{}, nil
	}

	decision, err := c.QuotaGate.Admit(ctx, ws)
	if err != nil {
		return false, reconcile.Result{RequeueAfter: quotaWaitRequeue}, err
	}
	if decision == nil {
		if err := c.removeAdmissionCondition(ctx, ws); err != nil {
			return false, reconcile.Result{}, err
		}
		return true, reconcile.Result{}, nil
	}

	if err := c.setAdmissionCondition(ctx, ws, decision); err != nil {
		return false, reconcile.Result{}, err
	}

	key := types.NamespacedName{Namespace: ws.Namespace, Name: ws.Name}
	switch decision.Phase {
	case quota.PhaseAdmitted:
		// The gate persists an annotation with an optimistic lock. Refresh the
		// object before continuing to create workloads.
		if err := c.Get(ctx, key, ws); err != nil {
			return false, reconcile.Result{}, err
		}
		return true, reconcile.Result{}, nil
	case quota.PhaseRejected:
		// Rejected is terminal with respect to automatic queue progression;
		// changing the claim or quota produces a new reconciliation.
		return false, reconcile.Result{}, nil
	default:
		requeue := decision.RequeueAfter
		if requeue <= 0 {
			requeue = quotaWaitRequeue
		}
		if decision.Blocked {
			requeue = quotaWaitRequeue
		}
		return false, reconcile.Result{RequeueAfter: requeue}, nil
	}
}

func (c *WorkspaceReconciler) removeAdmissionCondition(ctx context.Context, ws *kaitov1beta1.Workspace) error {
	if meta.FindStatusCondition(ws.Status.Conditions, string(kaitov1beta1.ConditionTypeAdmissionReady)) == nil {
		return nil
	}
	key := types.NamespacedName{Namespace: ws.Namespace, Name: ws.Name}
	return c.updateWorkspaceStatusIfChanged(ctx, key, func(status *kaitov1beta1.WorkspaceStatus) error {
		meta.RemoveStatusCondition(&status.Conditions, string(kaitov1beta1.ConditionTypeAdmissionReady))
		return nil
	})
}

func (c *WorkspaceReconciler) setAdmissionCondition(ctx context.Context, ws *kaitov1beta1.Workspace, decision *quota.Decision) error {
	key := types.NamespacedName{Namespace: ws.Namespace, Name: ws.Name}
	return c.updateWorkspaceStatusIfChanged(ctx, key, func(status *kaitov1beta1.WorkspaceStatus) error {
		if decision == nil {
			meta.RemoveStatusCondition(&status.Conditions, string(kaitov1beta1.ConditionTypeAdmissionReady))
			return nil
		}

		conditionStatus := metav1.ConditionFalse
		if decision.Phase == quota.PhaseAdmitted && decision.ResolutionError == "" {
			conditionStatus = metav1.ConditionTrue
		}
		reason := decision.Reason
		if decision.ResolutionError != "" {
			reason = quota.ReasonLimitResolutionFailed
		}
		if reason == "" {
			if decision.Phase == quota.PhaseAdmitted {
				reason = quota.ReasonAdmitted
			} else {
				reason = quota.ReasonClusterQuotaWaiting
			}
		}
		message := decision.Message
		if decision.ResolutionError != "" {
			if message != "" {
				message = decision.ResolutionError + "; using last known effective limit; " + message
			} else {
				message = decision.ResolutionError + "; using last known effective limit"
			}
		}
		if message == "" {
			message = string(decision.Phase)
		}
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               string(kaitov1beta1.ConditionTypeAdmissionReady),
			Status:             conditionStatus,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: ws.Generation,
		})
		return nil
	})
}
