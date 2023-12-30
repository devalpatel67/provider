package inventory

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	inventory "github.com/akash-network/akash-api/go/inventory/v1"
	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/akash-network/provider/cluster/kube/builder"
)

type nodeStateEnum int

const (
	daemonSetLabelSelector = "app=hostfeaturediscovery"
	daemonSetNamespace     = "akash-services"
	reconnectTimeout       = 5 * time.Second
)

const (
	nodeStateUpdated nodeStateEnum = iota
	nodeStateRemoved
)

type podStream struct {
	ctx              context.Context
	cancel           context.CancelFunc
	group            *errgroup.Group
	log              logr.Logger
	conn             *grpc.ClientConn
	stream           inventory.NodeRPC_QueryNodeClient
	broadcastStateCh chan<- nodeState
	nodeName         string
	address          string
	port             uint16
}

type nodeState struct {
	state nodeStateEnum
	name  string
	node  inventory.Node
}

type featureDiscovery struct {
	querierNodes
	ctx    context.Context
	group  *errgroup.Group
	log    logr.Logger
	kc     *kubernetes.Clientset
	nodeCh chan nodeState
}

func newFeatureDiscovery(ctx context.Context) *featureDiscovery {
	log := LogFromCtx(ctx).WithName("feature-discovery")

	group, ctx := errgroup.WithContext(ctx)

	fd := &featureDiscovery{
		log:          log,
		ctx:          logr.NewContext(ctx, log),
		group:        group,
		kc:           KubeClientFromCtx(ctx),
		querierNodes: newQuerierNodes(),
		nodeCh:       make(chan nodeState, 1),
	}

	group.Go(func() error {
		return fd.connectorRun()
	})

	group.Go(func() error {
		return fd.run()
	})

	return fd
}

func (fd *featureDiscovery) Wait() error {
	return fd.group.Wait()
}

func (fd *featureDiscovery) connectorRun() error {
	watcher, err := fd.kc.CoreV1().Pods(daemonSetNamespace).Watch(fd.ctx, metav1.ListOptions{
		LabelSelector: builder.AkashManagedLabelName + "=true" +
			",app.kubernetes.io/name=feature-discovery-node" +
			",app.kubernetes.io/component=inventory" +
			",app.kubernetes.io/part-of=operator",
	})

	if err != nil {
		return fmt.Errorf("error setting up Kubernetes watcher: %w", err)
	}

	nodes := make(map[string]*podStream)

	for {
		select {
		case <-fd.ctx.Done():
			for _, nd := range nodes {
				nd.cancel()
				delete(nodes, nd.nodeName)
			}

			return fd.ctx.Err()
		case event := <-watcher.ResultChan():
			if obj, valid := event.Object.(*corev1.Pod); valid {
				nodeName := obj.Spec.NodeName

				switch event.Type {
				case watch.Added:
					fallthrough
				case watch.Modified:
					if obj.Status.Phase == corev1.PodRunning && obj.Status.PodIP != "" {
						if _, exists := nodes[nodeName]; exists {
							continue
						}

						var containerPort uint16

						for _, container := range obj.Spec.Containers {
							if container.Name == fdContainerName {
								for _, port := range container.Ports {
									if port.Name == fdContainerPortName {
										containerPort = uint16(port.ContainerPort)
										break
									}
								}
								break
							}
						}

						nodes[nodeName] = newNodeWatcher(fd.ctx, nodeName, obj.Name, obj.Status.PodIP, containerPort, fd.nodeCh)
					}
				case watch.Deleted:
					nd, exists := nodes[nodeName]
					if !exists {
						continue
					}

					nd.cancel()
					delete(nodes, nodeName)
				}
			}
		}
	}
}

func (fd *featureDiscovery) run() error {
	nodes := make(map[string]inventory.Node)

	snapshot := func() inventory.Nodes {
		res := make(inventory.Nodes, 0, len(nodes))

		for _, nd := range nodes {
			res = append(res, nd.Dup())
		}

		return res
	}

	bus := PubSubFromCtx(fd.ctx)

	for {
		select {
		case <-fd.ctx.Done():
			return fd.ctx.Err()
		case evt := <-fd.nodeCh:
			switch evt.state {
			case nodeStateUpdated:
				nodes[evt.name] = evt.node
			case nodeStateRemoved:
				delete(nodes, evt.name)
			}

			bus.Pub(snapshot(), topicNodes)
		case req := <-fd.reqch:
			resp := respNodes{
				res: snapshot(),
			}

			req.respCh <- resp
		}
	}
}

func newNodeWatcher(ctx context.Context, nodeName string, podName string, address string, port uint16, evt chan<- nodeState) *podStream {
	ctx, cancel := context.WithCancel(ctx)
	group, ctx := errgroup.WithContext(ctx)

	ps := &podStream{
		ctx:              ctx,
		cancel:           cancel,
		group:            group,
		log:              LogFromCtx(ctx).WithName("node-watcher"),
		broadcastStateCh: evt,
		nodeName:         nodeName,
		address:          address,
		port:             port,
	}

	kubecfg := KubeConfigFromCtx(ctx)

	if kubecfg.BearerTokenFile != "/var/run/secrets/kubernetes.io/serviceaccount/token" {
		roundTripper, upgrader, err := spdy.RoundTripperFor(kubecfg)
		if err != nil {
			panic(err)
		}

		path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", daemonSetNamespace, podName)
		hostIP := strings.TrimPrefix(kubecfg.Host, "https://")
		serverURL := url.URL{Scheme: "https", Path: path, Host: hostIP}

		dialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, http.MethodPost, &serverURL)

		errch := make(chan error, 1)
		pf, err := portforward.New(dialer, []string{fmt.Sprintf(":%d", port)}, ctx.Done(), make(chan struct{}), os.Stdout, os.Stderr)
		if err != nil {
			panic(err)
		}

		group.Go(func() error {
			err := pf.ForwardPorts()
			errch <- err
			return err
		})

		select {
		case <-pf.Ready:
		case err := <-errch:
			panic(err)
		}

		ports, err := pf.GetPorts()
		if err != nil {
			panic(err)
		}

		ps.address = "localhost"
		ps.port = ports[0].Local
	}

	_ = ps.reconnect()

	return ps
}

// nolint: unused
func (nd *podStream) reconnect() error {
	select {
	case <-nd.ctx.Done():
		return nd.ctx.Err()
	default:
	}

	var err error
	// Establish the gRPC connection
	nd.conn, err = grpc.Dial(fmt.Sprintf("%s:%d", nd.address, nd.port), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		nd.log.Error(err, "couldn't dial endpoint")
		return err
	}

	client := inventory.NewNodeRPCClient(nd.conn)

	// Create a stream to receive updates from the node
	nd.stream, err = client.QueryNode(nd.ctx, &inventory.VoidNoParam{})
	if err != nil {
		nd.log.Error(err, "couldn't establish stream")
		return err
	}

	go nd.run()

	return nil
}

func (nd *podStream) run() {
	defer func() {
		_ = nd.conn.Close()
	}()

	for {
		node, err := nd.stream.Recv()
		if err != nil {
			select {
			case <-nd.ctx.Done():
				return
			case nd.broadcastStateCh <- nodeState{
				state: nodeStateRemoved,
				name:  nd.nodeName,
			}:
			}

			// if context is still alive then try to reconnect
			for {
				if err = nd.reconnect(); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}

					ctx, cancel := context.WithTimeout(nd.ctx, reconnectTimeout)
					select {
					case <-ctx.Done():
						err := ctx.Err()
						cancel()
						if !errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				}

				return
			}
		}

		select {
		case <-nd.ctx.Done():
			return
		case nd.broadcastStateCh <- nodeState{
			state: nodeStateUpdated,
			name:  nd.nodeName,
			node:  node.Dup(),
		}:
		}
	}
}
