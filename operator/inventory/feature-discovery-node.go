package inventory

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/cskr/pubsub"
	"github.com/go-logr/logr"
	"github.com/jaypipes/ghw/pkg/cpu"
	"github.com/jaypipes/ghw/pkg/gpu"
	"github.com/jaypipes/ghw/pkg/memory"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"

	v1 "github.com/akash-network/akash-api/go/inventory/v1"

	"github.com/akash-network/provider/cluster/kube/builder"
)

//go:embed gpu-info.json
var gpuDevs embed.FS

const (
	fdContainerName     = "inventory-node"
	fdContainerPortName = "api"
	topicNode           = "node"
	topicNodes          = "nodes"
)

type gpuDevice struct {
	Interface  string `json:"interface"`
	MemorySize string `json:"memory_size"`
}

type gpuDevices map[string]gpuDevice

type gpuVendor struct {
	Name    string     `json:"name"`
	Devices gpuDevices `json:"devices"`
}

type gpuVendors map[string]gpuVendor

type dpReqType int

const (
	dpReqCPU dpReqType = iota
	dpReqGPU
	dpReqMem
)

type dpReadResp struct {
	data interface{}
	err  error
}
type dpReadReq struct {
	op   dpReqType
	resp chan<- dpReadResp
}

type debuggerPod struct {
	ctx    context.Context
	readch chan dpReadReq
}

type Publisher interface {
	Pub(msg interface{}, topics ...string)
	TryPub(msg interface{}, topics ...string)
}

type Subscriber interface {
	Sub(topics ...string) chan interface{}
	SubOnce(topics ...string) chan interface{}
	SubOnceEach(topics ...string) chan interface{}
	AddSub(ch chan interface{}, topics ...string)
	AddSubOnceEach(ch chan interface{}, topics ...string)
	Unsub(ch chan interface{}, topics ...string)
}

type msgServiceServer struct {
	v1.NodeRPCServer
	ctx   context.Context
	log   logr.Logger
	sub   Subscriber
	reqch chan<- chan<- v1.Node
}

type fdNodeServer struct {
	ctx      context.Context
	log      logr.Logger
	reqch    <-chan chan<- v1.Node
	pub      Publisher
	nodeName string
}

var (
	supportedGPUs = gpuVendors{}
)

func init() {
	f, err := gpuDevs.Open("gpu-info.json")
	if err != nil {
		panic(err)
	}
	// close pci.ids file when done
	defer func() {
		_ = f.Close()
	}()

	data, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(data, &supportedGPUs)
	if err != nil {
		panic(err)
	}
}

func cmdFeatureDiscoveryNode() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "node",
		Short:        "feature discovery daemon-set node",
		Args:         cobra.ExactArgs(0),
		SilenceUsage: true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			kubecfg := KubeConfigFromCtx(cmd.Context())

			var hw hwInfo

			log := LogFromCtx(cmd.Context()).WithName("feature-discovery-node")

			if kubecfg.BearerTokenFile != "/var/run/secrets/kubernetes.io/serviceaccount/token" {
				log.Info("service is not running as kubernetes pod. starting debugger pod")

				dp := &debuggerPod{
					ctx:    cmd.Context(),
					readch: make(chan dpReadReq, 1),
				}

				group := ErrGroupFromCtx(cmd.Context())

				startch := make(chan struct{})

				group.Go(func() error {
					return dp.run(startch)
				})

				ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)

				select {
				case <-ctx.Done():
					if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
						return ctx.Err()
					}
				case <-startch:
					cancel()
				}

				hw = dp
			} else {
				hw = &localHwReader{}
			}

			CmdSetContextValue(cmd, CtxKeyHwInfo, hw)

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			log := LogFromCtx(ctx).WithName("feature-discovery-node")

			log.Info("starting k8s node features discovery")

			var err error

			podName := viper.GetString(FlagPodName)
			nodeName := viper.GetString(FlagNodeName)
			listenPort := viper.GetUint16(FlagGRPCPort)

			kc := KubeClientFromCtx(ctx)

			if listenPort == 0 {
				// this is dirty hack to discover exposed api port if this service runs within kubernetes
				podInfo, err := kc.CoreV1().Pods(daemonSetNamespace).Get(ctx, podName, metav1.GetOptions{})
				if err != nil {
					return err
				}

				for _, container := range podInfo.Spec.Containers {
					if container.Name == fdContainerName {
						for _, port := range container.Ports {
							if port.Name == fdContainerPortName {
								listenPort = uint16(port.ContainerPort)
							}
						}
					}
				}

				if listenPort == 0 {
					return fmt.Errorf("unable to detect pod's api port") // nolint: goerr113
				}
			}

			bus := pubsub.New(1000)

			endpoint := fmt.Sprintf(":%d", listenPort)

			log.Info(fmt.Sprintf("grpc listening on \"%s\"", endpoint))

			lis, err := net.Listen("tcp", endpoint)
			if err != nil {
				return err
			}

			srv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             30 * time.Second,
				PermitWithoutStream: false,
			}))

			reqch := make(chan chan<- v1.Node, 1)

			v1.RegisterNodeRPCServer(srv, &msgServiceServer{
				ctx:   ctx,
				log:   log.WithName("msg-srv"),
				sub:   bus,
				reqch: reqch,
			})

			reflection.Register(srv)

			group := ErrGroupFromCtx(ctx)

			fdns := &fdNodeServer{
				ctx:      ctx,
				log:      log.WithName("watcher"),
				reqch:    reqch,
				pub:      bus,
				nodeName: nodeName,
			}

			startch := make(chan struct{}, 1)
			group.Go(func() error {
				return fdns.run(startch)
			})

			select {
			case <-startch:
				group.Go(func() error {
					return srv.Serve(lis)
				})
			case <-ctx.Done():
				return ctx.Err()
			}

			select {
			case <-ctx.Done():
			}

			err = group.Wait()
			if !errors.Is(err, context.Canceled) {
				return err
			}

			return nil
		},
	}

	cmd.Flags().String(FlagPodName, "", "instance name")
	if err := viper.BindPFlag(FlagPodName, cmd.Flags().Lookup(FlagPodName)); err != nil {
		panic(err)
	}

	cmd.Flags().String(FlagNodeName, "", "node name")
	if err := viper.BindPFlag(FlagNodeName, cmd.Flags().Lookup(FlagNodeName)); err != nil {
		panic(err)
	}

	return cmd
}

func (nd *fdNodeServer) run(startch chan<- struct{}) error {
	kc := KubeClientFromCtx(nd.ctx)

	nodeWatch, err := kc.CoreV1().Nodes().Watch(nd.ctx, metav1.ListOptions{
		LabelSelector: builder.AkashManagedLabelName + "=true",
		FieldSelector: fields.OneTermEqualSelector(metav1.ObjectNameField, nd.nodeName).String(),
	})
	if err != nil {
		nd.log.Error(err, fmt.Sprintf("unable to start node watcher for \"%s\"", nd.nodeName))
		return err
	}

	defer nodeWatch.Stop()

	podsWatch, err := kc.CoreV1().Pods(corev1.NamespaceAll).Watch(nd.ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", nd.nodeName).String(),
	})
	if err != nil {
		nd.log.Error(err, "unable to fetch pods")
		return err
	}

	defer podsWatch.Stop()

	node, initPods, err := initNodeInfo(nd.ctx, nd.nodeName)
	if err != nil {
		nd.log.Error(err, "unable to init node info")
		return err
	}

	select {
	case <-nd.ctx.Done():
		return nd.ctx.Err()
	case startch <- struct{}{}:
	}

	signalch := make(chan struct{}, 1)
	signalch <- struct{}{}

	trySignal := func() {
		select {
		case signalch <- struct{}{}:
		default:
		}
	}

	for {
		select {
		case <-nd.ctx.Done():
			return nd.ctx.Err()
		case <-signalch:
			nd.pub.Pub(node.Dup(), topicNode)
		case req := <-nd.reqch:
			req <- node.Dup()
		case res := <-podsWatch.ResultChan():
			if pod, valid := res.Object.(*corev1.Pod); valid {
				switch res.Type {
				case watch.Added:
					if _, exists := initPods[pod.Name]; exists {
						delete(initPods, pod.Name)
					} else {
						for _, container := range pod.Spec.Containers {
							addAllocatedResources(&node, container.Resources.Requests)
						}
					}
				case watch.Deleted:
					if _, exists := initPods[pod.Name]; exists {
						delete(initPods, pod.Name)
					}

					for _, container := range pod.Spec.Containers {
						subAllocatedResources(&node, container.Resources.Requests)
					}
				}

				trySignal()
			}
		}
	}
}

func addAllocatedResources(node *v1.Node, rl corev1.ResourceList) {
	for name, quantity := range rl {
		switch name {
		case corev1.ResourceCPU:
			node.CPU.Quantity.Allocated.Add(quantity)
		case corev1.ResourceMemory:
			node.Memory.Quantity.Allocated.Add(quantity)
		case corev1.ResourceEphemeralStorage:
			node.EphemeralStorage.Quantity.Allocated.Add(quantity)
		case builder.ResourceGPUNvidia:
			fallthrough
		case builder.ResourceGPUAMD:
			node.GPU.Quantity.Allocated.Add(quantity)
		}
	}
}

func subAllocatedResources(node *v1.Node, rl corev1.ResourceList) {
	for name, quantity := range rl {
		switch name {
		case corev1.ResourceCPU:
			node.CPU.Quantity.Allocated.Sub(quantity)
		case corev1.ResourceMemory:
			node.Memory.Quantity.Allocated.Sub(quantity)
		case corev1.ResourceEphemeralStorage:
			node.EphemeralStorage.Quantity.Allocated.Sub(quantity)
		case builder.ResourceGPUNvidia:
			fallthrough
		case builder.ResourceGPUAMD:
			node.GPU.Quantity.Allocated.Sub(quantity)
		}
	}
}

func initNodeInfo(ctx context.Context, name string) (v1.Node, map[string]corev1.Pod, error) {
	kc := KubeClientFromCtx(ctx)

	cpuInfo, err := parseCPUInfo(ctx)
	if err != nil {
		return v1.Node{}, nil, err
	}

	gpuInfo, err := parseGPUInfo(ctx)
	if err != nil {
		return v1.Node{}, nil, err
	}

	res := v1.Node{
		CPU: v1.CPU{
			Quantity: v1.ResourcePair{
				Allocatable: resource.NewMilliQuantity(0, resource.DecimalSI),
				Allocated:   resource.NewMilliQuantity(0, resource.DecimalSI),
				Attributes:  nil,
			},
			Info: cpuInfo,
		},
		GPU: v1.GPU{
			Quantity: v1.ResourcePair{
				Allocatable: resource.NewQuantity(0, resource.DecimalSI),
				Allocated:   resource.NewQuantity(0, resource.DecimalSI),
			},
			Info: gpuInfo,
		},
		Memory: v1.Memory{
			Quantity: v1.ResourcePair{
				Allocatable: resource.NewQuantity(0, resource.DecimalSI),
				Allocated:   resource.NewQuantity(0, resource.DecimalSI),
			},
			Info: nil,
		},
		EphemeralStorage: v1.Storage{
			Quantity: v1.ResourcePair{
				Allocatable: resource.NewQuantity(0, resource.DecimalSI),
				Allocated:   resource.NewQuantity(0, resource.DecimalSI),
			},
			Info: v1.StorageInfo{},
		},
	}

	knode, err := kc.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return v1.Node{}, nil, fmt.Errorf("error fetching node %s: %w", name, err)
	}

	for name, r := range knode.Status.Allocatable {
		switch name {
		case corev1.ResourceCPU:
			val := r.DeepCopy()
			res.CPU.Quantity.Allocatable = &val
			res.CPU.Quantity.Allocated = resource.NewMilliQuantity(0, resource.DecimalSI)
		case corev1.ResourceMemory:
			val := r.DeepCopy()
			res.Memory.Quantity.Allocatable = &val
			res.Memory.Quantity.Allocated = resource.NewQuantity(0, resource.DecimalSI)
		case corev1.ResourceEphemeralStorage:
			val := r.DeepCopy()
			res.EphemeralStorage.Quantity.Allocatable = &val
			res.EphemeralStorage.Quantity.Allocated = resource.NewQuantity(0, resource.DecimalSI)
		case builder.ResourceGPUNvidia:
		case builder.ResourceGPUAMD:
			val := r.DeepCopy()
			res.GPU.Quantity.Allocatable = &val
			res.GPU.Quantity.Allocated = resource.NewQuantity(0, resource.DecimalSI)
		}
	}

	initPods := make(map[string]corev1.Pod)

	podsList, err := kc.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", name).String(),
	})
	if err != nil {
		return res, nil, err
	}

	for _, pod := range podsList.Items {
		for _, container := range pod.Spec.Containers {
			addAllocatedResources(&res, container.Resources.Requests)
		}
		initPods[pod.Name] = pod
	}

	return res, initPods, nil
}

func (s *msgServiceServer) QueryNode(_ *v1.VoidNoParam, stream v1.NodeRPC_QueryNodeServer) error {
	sendch := make(chan interface{}, 1)

	reqch := make(chan v1.Node, 1)

	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-stream.Context().Done():
		return stream.Context().Err()
	case s.reqch <- reqch:
	}

	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-stream.Context().Done():
		return stream.Context().Err()
	case sendch <- <-reqch:
	}

	subch := s.sub.Sub(topicNode)

	defer func() {
		s.sub.Unsub(subch, topicNode)
	}()

	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-stream.Context().Done():
			return stream.Context().Err()
		case nd := <-sendch:
			msg := nd.(v1.Node)
			if err := stream.Send(&msg); err != nil {
				return err
			}
		case msg := <-subch:
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-stream.Context().Done():
				return stream.Context().Err()
			case sendch <- msg:
			}
		}
	}
}

type hwInfo interface {
	CPU(context.Context) (*cpu.Info, error)
	GPU(context.Context) (*gpu.Info, error)
	Memory(context.Context) (*memory.Info, error)
}

type localHwReader struct{}

func (lfs *localHwReader) CPU(_ context.Context) (*cpu.Info, error) {
	return cpu.New()
}

func (lfs *localHwReader) GPU(_ context.Context) (*gpu.Info, error) {
	return gpu.New()
}

func (lfs *localHwReader) Memory(_ context.Context) (*memory.Info, error) {
	return memory.New()
}

func parseCPUInfo(ctx context.Context) (v1.CPUInfoS, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	hw := HWInfoFromCtx(ctx)

	cpus, err := hw.CPU(ctx)
	if err != nil {
		return nil, err
	}

	res := make(v1.CPUInfoS, 0, len(cpus.Processors))

	for _, c := range cpus.Processors {
		res = append(res, v1.CPUInfo{
			ID:     strconv.Itoa(c.ID),
			Vendor: c.Vendor,
			Model:  c.Model,
			Vcores: c.NumThreads,
		})
	}

	return res, nil
}

func parseGPUInfo(ctx context.Context) (v1.GPUInfoS, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	hw := HWInfoFromCtx(ctx)

	gpus, err := hw.GPU(ctx)
	if err != nil {
		return nil, err
	}

	res := make(v1.GPUInfoS, 0)

	for _, dev := range gpus.GraphicsCards {
		vendor, exists := supportedGPUs[dev.DeviceInfo.Vendor.ID]
		if !exists {
			continue
		}

		model, exists := vendor.Devices[dev.DeviceInfo.Product.ID]
		if !exists {
			continue
		}

		res = append(res, v1.GPUInfo{
			Vendor:     dev.DeviceInfo.Vendor.Name,
			Name:       dev.DeviceInfo.Product.Name,
			ModelID:    dev.DeviceInfo.Product.ID,
			Interface:  model.Interface,
			MemorySize: model.MemorySize,
		})
	}

	return res, nil
}

func (dp *debuggerPod) CPU(ctx context.Context) (*cpu.Info, error) {
	respch := make(chan dpReadResp, 1)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-dp.ctx.Done():
		return nil, dp.ctx.Err()
	case dp.readch <- dpReadReq{
		op:   dpReqCPU,
		resp: respch,
	}:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-dp.ctx.Done():
		return nil, dp.ctx.Err()
	case resp := <-respch:
		return resp.data.(*cpu.Info), resp.err
	}
}

func (dp *debuggerPod) GPU(ctx context.Context) (*gpu.Info, error) {
	respch := make(chan dpReadResp, 1)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-dp.ctx.Done():
		return nil, dp.ctx.Err()
	case dp.readch <- dpReadReq{
		op:   dpReqGPU,
		resp: respch,
	}:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-dp.ctx.Done():
		return nil, dp.ctx.Err()
	case resp := <-respch:
		return resp.data.(*gpu.Info), resp.err
	}
}

func (dp *debuggerPod) Memory(ctx context.Context) (*memory.Info, error) {
	respch := make(chan dpReadResp, 1)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-dp.ctx.Done():
		return nil, dp.ctx.Err()
	case dp.readch <- dpReadReq{
		op:   dpReqMem,
		resp: respch,
	}:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-dp.ctx.Done():
		return nil, dp.ctx.Err()
	case resp := <-respch:
		return resp.data.(*memory.Info), resp.err
	}
}

func (dp *debuggerPod) run(startch chan<- struct{}) error {
	log := LogFromCtx(dp.ctx)

	log.Info("staring debugger pod")

	req := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "fd-debugger-pod",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "psutil",
					Image: "ghcr.io/akash-network/provider-test:latest-arm64",
					Command: []string{
						"provider-services",
						"operator",
						"psutil",
						"serve",
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          "api",
							ContainerPort: 8081,
						},
					},
				},
			},
		},
	}

	kc := KubeClientFromCtx(dp.ctx)

	pod, err := kc.CoreV1().Pods(daemonSetNamespace).Create(dp.ctx, req, metav1.CreateOptions{})
	if err != nil && !kerrors.IsAlreadyExists(err) {
		return err
	}

	defer func() {
		// using default context here to delete pod as main might have been canceled
		_ = kc.CoreV1().Pods(daemonSetNamespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{})
	}()

	watcher, err := kc.CoreV1().Pods(daemonSetNamespace).Watch(dp.ctx, metav1.ListOptions{
		Watch:           true,
		ResourceVersion: pod.ResourceVersion,
		FieldSelector:   fields.Set{"metadata.name": pod.Name}.AsSelector().String(),
		LabelSelector:   labels.Everything().String(),
	})

	if err != nil {
		return err
	}

	defer func() {
		watcher.Stop()
	}()

	var apiPort int32

	for _, container := range pod.Spec.Containers {
		if container.Name == "psutil" {
			for _, port := range container.Ports {
				if port.Name == "api" {
					apiPort = port.ContainerPort
				}
			}
		}
	}

	if apiPort == 0 {
		return fmt.Errorf("debugger pod does not have port named \"api\"") // nolint: goerr113
	}

initloop:
	for {
		select {
		case <-dp.ctx.Done():
			return dp.ctx.Err()
		case evt := <-watcher.ResultChan():
			resp := evt.Object.(*corev1.Pod)
			if resp.Status.Phase != corev1.PodPending {
				watcher.Stop()
				startch <- struct{}{}
				break initloop
			}
		}
	}

	for {
		select {
		case <-dp.ctx.Done():
			return dp.ctx.Err()
		case readreq := <-dp.readch:
			var res string
			resp := dpReadResp{}

			switch readreq.op {
			case dpReqCPU:
				res = "cpu"
			case dpReqGPU:
				res = "gpu"
			case dpReqMem:
				res = "memory"
			}

			result := kc.CoreV1().RESTClient().Get().
				Namespace(daemonSetNamespace).
				Resource("pods").
				Name(fmt.Sprintf("%s:%d", pod.Name, apiPort)).
				SubResource("proxy").
				Suffix(res).
				Do(dp.ctx)

			resp.err = result.Error()

			if resp.err == nil {
				var data []byte
				data, resp.err = result.Raw()
				if resp.err == nil {
					switch readreq.op {
					case dpReqCPU:
						var res cpu.Info
						resp.err = json.Unmarshal(data, &res)
						resp.data = &res
					case dpReqGPU:
						var res gpu.Info
						resp.err = json.Unmarshal(data, &res)
						resp.data = &res
					case dpReqMem:
						var res memory.Info
						resp.err = json.Unmarshal(data, &res)
						resp.data = &res
					}
				}
			}

			readreq.resp <- resp
		}
	}
}

// // ExecCmd exec command on specific pod and wait the command's output.
// func ExecCmd(ctx context.Context, podName string, command string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
// 	kc := KubeClientFromCtx(ctx)
// 	cfg := KubeConfigFromCtx(ctx)
//
// 	cmd := []string{
// 		"sh",
// 		"-c",
// 		command,
// 	}
//
// 	option := &corev1.PodExecOptions{
// 		Command: cmd,
// 		Stdin:   true,
// 		Stdout:  true,
// 		Stderr:  true,
// 		TTY:     true,
// 	}
// 	if stdin == nil {
// 		option.Stdin = false
// 	}
//
// 	req := kc.CoreV1().
// 		RESTClient().
// 		Post().
// 		Resource("pods").
// 		Name(podName).
// 		Namespace(daemonSetNamespace).
// 		SubResource("exec").
// 		VersionedParams(option, scheme.ParameterCodec)
//
// 	exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
// 	if err != nil {
// 		return err
// 	}
// 	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
// 		Stdin:  stdin,
// 		Stdout: stdout,
// 		Stderr: stderr,
// 	})
// 	if err != nil {
// 		return err
// 	}
//
// 	return nil
// }
