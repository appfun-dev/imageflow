package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/appfun-dev/imageflow/internal/clock"
)

func init() {
	Registrars = append(Registrars, &noopRegistrar{})
}

type noopRegistrar struct{}

func (noopRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.ConfigMap{}).
		Named("noop").
		Complete(&noopReconciler{client: mgr.GetClient(), clock: opts.Clock})
}

type noopReconciler struct {
	client client.Client
	clock  clock.Clock
}

func (r *noopReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log.FromContext(ctx).Info("noop reconcile", "namespace", req.Namespace, "name", req.Name, "now", r.clock.Now())
	return ctrl.Result{}, nil
}
