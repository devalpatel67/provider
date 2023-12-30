package inventory

import (
	"context"

	inventory "github.com/akash-network/akash-api/go/inventory/v1"
	"github.com/cskr/pubsub"

	rookexec "github.com/rook/rook/pkg/util/exec"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	providerflags "github.com/akash-network/provider/cmd/provider-services/cmd/flags"
	akashv2beta2 "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
)

const (
	FlagAPITimeout   = "api-timeout"
	FlagQueryTimeout = "query-timeout"
	FlagRESTPort     = "rest-port"
	FlagGRPCPort     = "grpc-port"
	FlagPodName      = "pod-name"
	FlagNodeName     = "node-name"
)

type ContextKey string

const (
	CtxKeyKubeConfig       = ContextKey(providerflags.FlagKubeConfig)
	CtxKeyKubeClientSet    = ContextKey("kube-clientset")
	CtxKeyRookClientSet    = ContextKey("rook-clientset")
	CtxKeyAkashClientSet   = ContextKey("akash-clientset")
	CtxKeyPubSub           = ContextKey("pubsub")
	CtxKeyLifecycle        = ContextKey("lifecycle")
	CtxKeyErrGroup         = ContextKey("errgroup")
	CtxKeyStorage          = ContextKey("storage")
	CtxKeyFeatureDiscovery = ContextKey("feature-discovery")
	CtxKeyInformersFactory = ContextKey("informers-factory")
	CtxKeyHwInfo           = ContextKey("hardware-info")
)

type respStorage struct {
	res []akashv2beta2.InventoryClusterStorage
	err error
}

type respNodes struct {
	res inventory.Nodes
	err error
}

type reqStorage struct {
	respCh chan respStorage
}

type reqNodes struct {
	respCh chan respNodes
}

type querierStorage struct {
	reqch chan reqStorage
}

type querierNodes struct {
	reqch chan reqNodes
}

func newQuerierStorage() querierStorage {
	return querierStorage{
		reqch: make(chan reqStorage, 100),
	}
}

func newQuerierNodes() querierNodes {
	return querierNodes{
		reqch: make(chan reqNodes, 100),
	}
}

func (c *querierStorage) Query(ctx context.Context) ([]akashv2beta2.InventoryClusterStorage, error) {
	r := reqStorage{
		respCh: make(chan respStorage, 1),
	}

	select {
	case c.reqch <- r:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case rsp := <-r.respCh:
		return rsp.res, rsp.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *querierNodes) Query(ctx context.Context) (inventory.Nodes, error) {
	r := reqNodes{
		respCh: make(chan respNodes, 1),
	}

	select {
	case c.reqch <- r:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case rsp := <-r.respCh:
		return rsp.res, rsp.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type QuerierStorage interface {
	Query(ctx context.Context) ([]akashv2beta2.InventoryClusterStorage, error)
}

type QuerierNodes interface {
	Query(ctx context.Context) (inventory.Nodes, error)
}

type Watcher interface {
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

type RemotePodCommandExecutor interface {
	ExecWithOptions(ctx context.Context, options rookexec.ExecOptions) (string, string, error)
	ExecCommandInContainerWithFullOutput(ctx context.Context, appLabel, containerName, namespace string, cmd ...string) (string, string, error)
	// ExecCommandInContainerWithFullOutputWithTimeout uses 15s hard-coded timeout
	ExecCommandInContainerWithFullOutputWithTimeout(ctx context.Context, appLabel, containerName, namespace string, cmd ...string) (string, string, error)
}

func NewRemotePodCommandExecutor(restcfg *rest.Config, clientset *kubernetes.Clientset) RemotePodCommandExecutor {
	return &rookexec.RemotePodCommandExecutor{
		ClientSet:  clientset,
		RestClient: restcfg,
	}
}

func InformKubeObjects(ctx context.Context, pubsub *pubsub.PubSub, informer cache.SharedIndexInformer, topic string) {
	ErrGroupFromCtx(ctx).Go(func() error {
		_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pubsub.Pub(watch.Event{
					Type:   watch.Added,
					Object: obj.(runtime.Object),
				}, topic)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				pubsub.Pub(watch.Event{
					Type:   watch.Modified,
					Object: newObj.(runtime.Object),
				}, topic)
			},
			DeleteFunc: func(obj interface{}) {
				pubsub.Pub(watch.Event{
					Type:   watch.Deleted,
					Object: obj.(runtime.Object),
				}, topic)
			},
		})

		if err != nil {
			LogFromCtx(ctx).Error(err, "couldn't register event handlers")
			return nil
		}

		informer.Run(ctx.Done())

		return nil
	})
}
