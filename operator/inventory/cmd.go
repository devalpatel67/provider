package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/cskr/pubsub"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/gorilla/mux"
	rookclientset "github.com/rook/rook/pkg/client/clientset/versioned"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	inventory "github.com/akash-network/akash-api/go/inventory/v1"

	"github.com/akash-network/node/util/runner"

	"github.com/akash-network/provider/cluster/kube/clientcommon"
	providerflags "github.com/akash-network/provider/cmd/provider-services/cmd/flags"
	cmdutil "github.com/akash-network/provider/cmd/provider-services/cmd/util"
	akashv2beta2 "github.com/akash-network/provider/pkg/apis/akash.network/v2beta2"
	akashclientset "github.com/akash-network/provider/pkg/client/clientset/versioned"
)

type router struct {
	*mux.Router
	queryTimeout time.Duration
}

type grpcMsgService struct {
	inventory.ClusterRPCServer
	ctx context.Context
}

func CmdSetContextValue(cmd *cobra.Command, key, val interface{}) {
	cmd.SetContext(context.WithValue(cmd.Context(), key, val))
}

func Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "inventory",
		Short:        "kubernetes operator interfacing inventory",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			zconf := zap.NewDevelopmentConfig()
			zconf.DisableCaller = true
			zconf.EncoderConfig.EncodeTime = func(time.Time, zapcore.PrimitiveArrayEncoder) {}

			zapLog, _ := zconf.Build()

			group, ctx := errgroup.WithContext(cmd.Context())

			cmd.SetContext(logr.NewContext(ctx, zapr.NewLogger(zapLog)))

			if err := loadKubeConfig(cmd); err != nil {
				return err
			}

			kubecfg := KubeConfigFromCtx(cmd.Context())

			kc, err := kubernetes.NewForConfig(kubecfg)
			if err != nil {
				return err
			}

			CmdSetContextValue(cmd, CtxKeyKubeConfig, kubecfg)
			CmdSetContextValue(cmd, CtxKeyKubeClientSet, kc)
			CmdSetContextValue(cmd, CtxKeyErrGroup, group)

			// lc := lifecycle.New()
			//
			// lc.WatchContext(cmd.Context())
			//
			// CmdSetContextValue(cmd, CtxKeyLifecycle, lc)

			return nil
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			kubecfg := KubeConfigFromCtx(cmd.Context())

			rc, err := rookclientset.NewForConfig(kubecfg)
			if err != nil {
				return err
			}

			ac, err := akashclientset.NewForConfig(kubecfg)
			if err != nil {
				return err
			}

			CmdSetContextValue(cmd, CtxKeyRookClientSet, rc)
			CmdSetContextValue(cmd, CtxKeyAkashClientSet, ac)
			CmdSetContextValue(cmd, CtxKeyPubSub, pubsub.New(1000))

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			bus := PubSubFromCtx(ctx)
			group := ErrGroupFromCtx(ctx)

			var storage []QuerierStorage
			st, err := NewCeph(ctx)
			if err != nil {
				return err
			}
			storage = append(storage, st)

			if st, err = NewRancher(ctx); err != nil {
				return err
			}

			fd := newFeatureDiscovery(ctx)

			storage = append(storage, st)

			CmdSetContextValue(cmd, CtxKeyStorage, storage)
			CmdSetContextValue(cmd, CtxKeyFeatureDiscovery, fd)

			apiTimeout := viper.GetDuration(FlagAPITimeout)
			queryTimeout := viper.GetDuration(FlagQueryTimeout)
			restPort := viper.GetUint16(FlagRESTPort)
			grpcPort := viper.GetUint16(FlagGRPCPort)

			srv := &http.Server{
				Addr:    fmt.Sprintf(":%d", restPort),
				Handler: newRouter(LogFromCtx(ctx).WithName("router"), apiTimeout, queryTimeout),
				BaseContext: func(_ net.Listener) context.Context {
					return cmd.Context()
				},
				ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout:       60 * time.Second,
			}

			grpcSrv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             30 * time.Second,
				PermitWithoutStream: false,
			}))

			inventory.RegisterClusterRPCServer(grpcSrv, &grpcMsgService{
				ctx: ctx,
			})

			reflection.Register(grpcSrv)

			group.Go(func() error {
				return fd.Wait()
			})

			group.Go(func() error {
				return srv.ListenAndServe()
			})

			group.Go(func() error {
				grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
				if err != nil {
					return err
				}

				return srv.Serve(grpcLis)
			})

			group.Go(func() error {
				<-ctx.Done()
				return srv.Shutdown(ctx)
			})

			kc := KubeClientFromCtx(ctx)
			factory := informers.NewSharedInformerFactory(kc, 0)

			InformKubeObjects(ctx,
				bus,
				factory.Core().V1().Namespaces().Informer(),
				"ns")

			InformKubeObjects(ctx,
				bus,
				factory.Storage().V1().StorageClasses().Informer(),
				"sc")

			InformKubeObjects(ctx,
				bus,
				factory.Core().V1().PersistentVolumes().Informer(),
				"pv")

			InformKubeObjects(ctx,
				bus,
				factory.Core().V1().Nodes().Informer(),
				"nodes")

			return group.Wait()
		},
	}

	err := providerflags.AddKubeConfigPathFlag(cmd)
	if err != nil {
		panic(err)
	}

	cmd.Flags().Duration(FlagAPITimeout, 3*time.Second, "api timeout")
	if err = viper.BindPFlag(FlagAPITimeout, cmd.Flags().Lookup(FlagAPITimeout)); err != nil {
		panic(err)
	}

	cmd.Flags().Duration(FlagQueryTimeout, 2*time.Second, "query timeout")
	if err = viper.BindPFlag(FlagQueryTimeout, cmd.Flags().Lookup(FlagQueryTimeout)); err != nil {
		panic(err)
	}

	cmd.Flags().Uint16(FlagRESTPort, 8080, "port to REST api")
	if err = viper.BindPFlag(FlagRESTPort, cmd.Flags().Lookup(FlagRESTPort)); err != nil {
		panic(err)
	}

	cmd.Flags().Uint16(FlagGRPCPort, 8081, "port to GRPC api")
	if err = viper.BindPFlag(FlagGRPCPort, cmd.Flags().Lookup(FlagGRPCPort)); err != nil {
		panic(err)
	}

	cmd.AddCommand(cmdFeatureDiscoveryNode())

	return cmd
}

func loadKubeConfig(c *cobra.Command) error {
	configPath, _ := c.Flags().GetString(providerflags.FlagKubeConfig)

	config, err := clientcommon.OpenKubeConfig(configPath, cmdutil.OpenLogger().With("cmp", "provider"))
	if err != nil {
		return err
	}

	CmdSetContextValue(c, CtxKeyKubeConfig, config)

	return nil
}

func newRouter(_ logr.Logger, apiTimeout, queryTimeout time.Duration) *router {
	mRouter := mux.NewRouter()
	rt := &router{
		Router:       mRouter,
		queryTimeout: queryTimeout,
	}

	mRouter.Use(func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rCtx, cancel := context.WithTimeout(r.Context(), apiTimeout)
			defer cancel()

			h.ServeHTTP(w, r.WithContext(rCtx))
		})
	})

	mRouter.HandleFunc("/inventory", rt.legacyInventoryHandler)

	inventoryRouter := mRouter.PathPrefix("/v1").Subrouter()
	inventoryRouter.HandleFunc("/inventory", rt.InventoryHandler)

	return rt
}

func (rt *router) InventoryHandler(w http.ResponseWriter, req *http.Request) {
	storage := StorageFromCtx(req.Context())
	fd := FeatureDiscoveryFromCtx(req.Context())

	var data []byte

	ctx, cancel := context.WithTimeout(req.Context(), rt.queryTimeout)
	defer func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			w.WriteHeader(http.StatusRequestTimeout)
		}

		if len(data) > 0 {
			_, _ = w.Write(data)
		}
	}()

	datach := make(chan runner.Result, 1)
	var wg sync.WaitGroup

	wg.Add(len(storage) + 1)

	for idx := range storage {
		go func(idx int) {
			defer wg.Done()

			datach <- runner.NewResult(storage[idx].Query(ctx))
		}(idx)
	}

	go func() {
		defer wg.Done()
		datach <- runner.NewResult(fd.Query(ctx))
	}()

	go func() {
		defer cancel()
		wg.Wait()
	}()

	inv := inventory.Cluster{}

done:
	for {
		select {
		case <-ctx.Done():
			break done
		case res := <-datach:
			switch obj := res.Value().(type) {
			case []akashv2beta2.InventoryClusterStorage:
				for _, s := range obj {
					nS := inventory.Storage{
						Quantity: inventory.ResourcePair{
							Allocatable: resource.NewQuantity(int64(s.Allocatable), resource.DecimalSI),
							Allocated:   resource.NewQuantity(int64(s.Allocated), resource.DecimalSI),
							Attributes:  nil,
						},
						Info: inventory.StorageInfo{
							Class: s.Class,
						},
					}

					inv.Storage = append(inv.Storage, nS)
				}
			case inventory.Nodes:
				inv.Nodes = obj.Dup()
			}
		}
	}

	var err error
	if data, err = json.Marshal(&inv); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		data = []byte(err.Error())
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
}

func (rt *router) legacyInventoryHandler(w http.ResponseWriter, req *http.Request) {
	storage := StorageFromCtx(req.Context())
	inv := akashv2beta2.Inventory{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Inventory",
			APIVersion: "akash.network/v2beta2",
		},
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Now().UTC()),
		},
		Spec: akashv2beta2.InventorySpec{},
		Status: akashv2beta2.InventoryStatus{
			State: akashv2beta2.InventoryStatePulled,
		},
	}

	var data []byte

	ctx, cancel := context.WithTimeout(req.Context(), rt.queryTimeout)
	defer func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			w.WriteHeader(http.StatusRequestTimeout)
		}

		if len(data) > 0 {
			_, _ = w.Write(data)
		}
	}()

	datach := make(chan runner.Result, 1)
	var wg sync.WaitGroup

	wg.Add(len(storage) + 1)

	for idx := range storage {
		go func(idx int) {
			defer wg.Done()

			datach <- runner.NewResult(storage[idx].Query(ctx))
		}(idx)
	}

	go func() {
		defer cancel()
		wg.Wait()
	}()

done:
	for {
		select {
		case <-ctx.Done():
			break done
		case res := <-datach:
			if res.Error() != nil {
				inv.Status.Messages = append(inv.Status.Messages, res.Error().Error())
			}

			if inventory, valid := res.Value().([]akashv2beta2.InventoryClusterStorage); valid {
				inv.Spec.Storage = append(inv.Spec.Storage, inventory...)
			}
		}
	}

	var err error
	if data, err = json.Marshal(&inv); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		data = []byte(err.Error())
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
}

func (gm *grpcMsgService) QueryCluster(_ *inventory.VoidNoParam, stream inventory.ClusterRPC_QueryClusterServer) error {
	bus := PubSubFromCtx(gm.ctx)

	sendch := make(chan interface{}, 1)

	// reqch := make(chan inventory.Nodes, 1)

	// select {
	// case <-s.ctx.Done():
	// 	return s.ctx.Err()
	// case <-stream.Context().Done():
	// 	return stream.Context().Err()
	// case s.reqch <- reqch:
	// }
	//
	// select {
	// case <-s.ctx.Done():
	// 	return s.ctx.Err()
	// case <-stream.Context().Done():
	// 	return stream.Context().Err()
	// case sendch <- <-reqch:
	// }

	subch := bus.Sub(topicNodes)

	defer func() {
		bus.Unsub(subch, topicNodes)
	}()

	var state inventory.Cluster

	for {
		select {
		case <-gm.ctx.Done():
			return gm.ctx.Err()
		case <-stream.Context().Done():
			return stream.Context().Err()
		case msg := <-sendch:
			switch obj := msg.(type) {
			case inventory.Nodes:
				state.Nodes = obj
			case inventory.ClusterStorage:
				state.Storage = obj
			}

			if err := stream.Send(&state); err != nil {
				return err
			}
		case msg := <-subch:
			select {
			case <-gm.ctx.Done():
				return gm.ctx.Err()
			case <-stream.Context().Done():
				return stream.Context().Err()
			case sendch <- msg:
			}
		}
	}
}
