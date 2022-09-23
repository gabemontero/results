package taskrun

import (
	"context"
	"time"

	"knative.dev/pkg/controller"
	knativereconciler "knative.dev/pkg/reconciler"

	"github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	"github.com/tektoncd/pipeline/pkg/client/listers/pipeline/v1beta1"
	"github.com/tektoncd/results/pkg/watcher/reconciler"
	"github.com/tektoncd/results/pkg/watcher/reconciler/dynamic"
	pb "github.com/tektoncd/results/proto/v1alpha2/results_go_proto"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/logging"
)

type Reconciler struct {
	// Inline LeaderAwareFuncs to support leader election.
	knativereconciler.LeaderAwareFuncs

	client    pb.ResultsClient
	lister    v1beta1.TaskRunLister
	k8sclient versioned.Interface
	cfg       *reconciler.Config
	enqueue   func(interface{}, time.Duration)
}

// Check that our Reconciler is LeaderAware.
var _ knativereconciler.LeaderAware = (*Reconciler)(nil)

func (r *Reconciler) Reconcile(ctx context.Context, key string) error {
	log := logging.FromContext(ctx)
	log.With(zap.String("key", key))

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		log.Errorf("invalid resource key: %s", key)
		return nil
	}

	if !r.IsLeaderFor(types.NamespacedName{Namespace: namespace, Name: name}) {
		log.Info("Skipping TaskRun key because this instance isn't its leader")
		return controller.NewSkipKey(key)
	}

	tr, err := r.lister.TaskRuns(namespace).Get(name)
	if err != nil {
		return err
	}

	k8sclient := &dynamic.TaskRunClient{
		TaskRunInterface: r.k8sclient.TektonV1beta1().TaskRuns(namespace),
	}

	dyn := dynamic.NewDynamicReconciler(r.client, k8sclient, r.cfg, r.enqueue)
	if err := dyn.Reconcile(ctx, tr); err != nil {
		return err
	}

	return nil
}
