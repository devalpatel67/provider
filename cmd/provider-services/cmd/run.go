package cmd

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	sdkclient "github.com/cosmos/cosmos-sdk/client"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/tendermint/tendermint/libs/log"

	"github.com/cosmos/cosmos-sdk/client/flags"
	sdk "github.com/cosmos/cosmos-sdk/types"

	ctypes "github.com/akash-network/akash-api/go/node/cert/v1beta3"
	mparams "github.com/akash-network/akash-api/go/node/market/v1beta4"
	ptypes "github.com/akash-network/akash-api/go/node/provider/v1beta3"
	"github.com/akash-network/node/cmd/common"
	"github.com/akash-network/node/events"
	"github.com/akash-network/node/pubsub"
	"github.com/akash-network/node/sdl"
	cutils "github.com/akash-network/node/x/cert/utils"
	config2 "github.com/akash-network/node/x/provider/config"

	"github.com/akash-network/provider"
	"github.com/akash-network/provider/bidengine"
	"github.com/akash-network/provider/client"
	"github.com/akash-network/provider/cluster"
	"github.com/akash-network/provider/cluster/kube"
	"github.com/akash-network/provider/cluster/kube/builder"
	"github.com/akash-network/provider/cluster/kube/clientcommon"
	"github.com/akash-network/provider/cluster/operatorclients"
	providerflags "github.com/akash-network/provider/cmd/provider-services/cmd/flags"
	cmdutil "github.com/akash-network/provider/cmd/provider-services/cmd/util"
	gwrest "github.com/akash-network/provider/gateway/rest"
	"github.com/akash-network/provider/operator/waiter"
	"github.com/akash-network/provider/session"
)

const (
	// FlagClusterK8s informs the provider to scan and utilize localized kubernetes client configuration
	FlagClusterK8s = "cluster-k8s"

	// FlagGatewayListenAddress determines listening address for Manifests
	FlagGatewayListenAddress             = "gateway-listen-address"
	FlagBidPricingStrategy               = "bid-price-strategy"
	FlagBidPriceCPUScale                 = "bid-price-cpu-scale"
	FlagBidPriceMemoryScale              = "bid-price-memory-scale"
	FlagBidPriceStorageScale             = "bid-price-storage-scale"
	FlagBidPriceEndpointScale            = "bid-price-endpoint-scale"
	FlagBidPriceScriptPath               = "bid-price-script-path"
	FlagBidPriceScriptProcessLimit       = "bid-price-script-process-limit"
	FlagBidPriceScriptTimeout            = "bid-price-script-process-timeout"
	FlagBidDeposit                       = "bid-deposit"
	FlagClusterPublicHostname            = "cluster-public-hostname"
	FlagClusterNodePortQuantity          = "cluster-node-port-quantity"
	FlagClusterWaitReadyDuration         = "cluster-wait-ready-duration"
	FlagInventoryResourcePollPeriod      = "inventory-resource-poll-period"
	FlagInventoryResourceDebugFrequency  = "inventory-resource-debug-frequency"
	FlagDeploymentIngressStaticHosts     = "deployment-ingress-static-hosts"
	FlagDeploymentIngressDomain          = "deployment-ingress-domain"
	FlagDeploymentIngressExposeLBHosts   = "deployment-ingress-expose-lb-hosts"
	FlagDeploymentNetworkPoliciesEnabled = "deployment-network-policies-enabled"
	FlagDockerImagePullSecretsName       = "docker-image-pull-secrets-name" // nolint: gosec
	FlagOvercommitPercentMemory          = "overcommit-pct-mem"
	FlagOvercommitPercentCPU             = "overcommit-pct-cpu"
	FlagOvercommitPercentStorage         = "overcommit-pct-storage"
	FlagDeploymentBlockedHostnames       = "deployment-blocked-hostnames"
	FlagAuthPem                          = "auth-pem"
	FlagDeploymentRuntimeClass           = "deployment-runtime-class"
	FlagBidTimeout                       = "bid-timeout"
	FlagManifestTimeout                  = "manifest-timeout"
	FlagMetricsListener                  = "metrics-listener"
	FlagWithdrawalPeriod                 = "withdrawal-period"
	FlagLeaseFundsMonitorInterval        = "lease-funds-monitor-interval"
	FlagMinimumBalance                   = "minimum-balance"
	FlagProviderConfig                   = "provider-config"
	FlagCachedResultMaxAge               = "cached-result-max-age"
	FlagRPCQueryTimeout                  = "rpc-query-timeout"
	FlagBidPriceIPScale                  = "bid-price-ip-scale"
	FlagEnableIPOperator                 = "ip-operator"
	FlagTxBroadcastTimeout               = "tx-broadcast-timeout"
)

const (
	serviceIPOperator       = "ip-operator"
	serviceHostnameOperator = "hostname-operator"
)

var (
	errInvalidConfig = errors.New("Invalid configuration")
)

// RunCmd launches the Akash Provider service
func RunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "run",
		Short:        "run akash provider",
		SilenceUsage: true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			leaseFundsMonInterval := viper.GetDuration(FlagLeaseFundsMonitorInterval)
			withdrawPeriod := viper.GetDuration(FlagWithdrawalPeriod)

			if leaseFundsMonInterval < time.Minute || leaseFundsMonInterval > 24*time.Hour {
				return errors.Errorf(`flag "%s" contains invalid value. expected >=1m<=24h`, FlagLeaseFundsMonitorInterval) // nolint: goerr113
			}

			if withdrawPeriod > 0 && withdrawPeriod < leaseFundsMonInterval {
				return errors.Errorf(`flag "%s" value must be > "%s"`, FlagWithdrawalPeriod, FlagLeaseFundsMonitorInterval) // nolint: goerr113
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return common.RunForeverWithContext(cmd.Context(), func(ctx context.Context) error {
				return doRunCmd(ctx, cmd, args)
			})
		},
	}

	cmd.Flags().String(flags.FlagChainID, "", "The network chain ID")
	if err := viper.BindPFlag(flags.FlagChainID, cmd.Flags().Lookup(flags.FlagChainID)); err != nil {
		return nil
	}

	flags.AddTxFlagsToCmd(cmd)

	cfg := provider.NewDefaultConfig()

	cmd.Flags().Bool(FlagClusterK8s, false, "Use Kubernetes cluster")
	if err := viper.BindPFlag(FlagClusterK8s, cmd.Flags().Lookup(FlagClusterK8s)); err != nil {
		return nil
	}

	cmd.Flags().String(providerflags.FlagK8sManifestNS, "lease", "Cluster manifest namespace")
	if err := viper.BindPFlag(providerflags.FlagK8sManifestNS, cmd.Flags().Lookup(providerflags.FlagK8sManifestNS)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagGatewayListenAddress, "0.0.0.0:8443", "Gateway listen address")
	if err := viper.BindPFlag(FlagGatewayListenAddress, cmd.Flags().Lookup(FlagGatewayListenAddress)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagBidPricingStrategy, "scale", "Pricing strategy to use")
	if err := viper.BindPFlag(FlagBidPricingStrategy, cmd.Flags().Lookup(FlagBidPricingStrategy)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagBidPriceCPUScale, "0", "cpu pricing scale in uakt per millicpu")
	if err := viper.BindPFlag(FlagBidPriceCPUScale, cmd.Flags().Lookup(FlagBidPriceCPUScale)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagBidPriceMemoryScale, "0", "memory pricing scale in uakt per megabyte")
	if err := viper.BindPFlag(FlagBidPriceMemoryScale, cmd.Flags().Lookup(FlagBidPriceMemoryScale)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagBidPriceStorageScale, "0", "storage pricing scale in uakt per megabyte")
	if err := viper.BindPFlag(FlagBidPriceStorageScale, cmd.Flags().Lookup(FlagBidPriceStorageScale)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagBidPriceEndpointScale, "0", "endpoint pricing scale in uakt")
	if err := viper.BindPFlag(FlagBidPriceEndpointScale, cmd.Flags().Lookup(FlagBidPriceEndpointScale)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagBidPriceIPScale, "0", "leased ip pricing scale in uakt")
	if err := viper.BindPFlag(FlagBidPriceIPScale, cmd.Flags().Lookup(FlagBidPriceIPScale)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagBidPriceScriptPath, "", "path to script to run for computing bid price")
	if err := viper.BindPFlag(FlagBidPriceScriptPath, cmd.Flags().Lookup(FlagBidPriceScriptPath)); err != nil {
		return nil
	}

	cmd.Flags().Uint(FlagBidPriceScriptProcessLimit, 32, "limit to the number of scripts run concurrently for bid pricing")
	if err := viper.BindPFlag(FlagBidPriceScriptProcessLimit, cmd.Flags().Lookup(FlagBidPriceScriptProcessLimit)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagBidPriceScriptTimeout, time.Second*10, "execution timelimit for bid pricing as a duration")
	if err := viper.BindPFlag(FlagBidPriceScriptTimeout, cmd.Flags().Lookup(FlagBidPriceScriptTimeout)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagBidDeposit, cfg.BidDeposit.String(), "Bid deposit amount")
	if err := viper.BindPFlag(FlagBidDeposit, cmd.Flags().Lookup(FlagBidDeposit)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagClusterPublicHostname, "", "The public IP of the Kubernetes cluster")
	if err := viper.BindPFlag(FlagClusterPublicHostname, cmd.Flags().Lookup(FlagClusterPublicHostname)); err != nil {
		return nil
	}

	cmd.Flags().Uint(FlagClusterNodePortQuantity, 1, "The number of node ports available on the Kubernetes cluster")
	if err := viper.BindPFlag(FlagClusterNodePortQuantity, cmd.Flags().Lookup(FlagClusterNodePortQuantity)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagClusterWaitReadyDuration, time.Second*5, "The time to wait for the cluster to be available")
	if err := viper.BindPFlag(FlagClusterWaitReadyDuration, cmd.Flags().Lookup(FlagClusterWaitReadyDuration)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagInventoryResourcePollPeriod, time.Second*5, "The period to poll the cluster inventory")
	if err := viper.BindPFlag(FlagInventoryResourcePollPeriod, cmd.Flags().Lookup(FlagInventoryResourcePollPeriod)); err != nil {
		return nil
	}

	cmd.Flags().Uint(FlagInventoryResourceDebugFrequency, 10, "The rate at which to log all inventory resources")
	if err := viper.BindPFlag(FlagInventoryResourceDebugFrequency, cmd.Flags().Lookup(FlagInventoryResourceDebugFrequency)); err != nil {
		return nil
	}

	cmd.Flags().Bool(FlagDeploymentIngressStaticHosts, false, "")
	if err := viper.BindPFlag(FlagDeploymentIngressStaticHosts, cmd.Flags().Lookup(FlagDeploymentIngressStaticHosts)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagDeploymentIngressDomain, "", "")
	if err := viper.BindPFlag(FlagDeploymentIngressDomain, cmd.Flags().Lookup(FlagDeploymentIngressDomain)); err != nil {
		return nil
	}

	cmd.Flags().Bool(FlagDeploymentIngressExposeLBHosts, false, "")
	if err := viper.BindPFlag(FlagDeploymentIngressExposeLBHosts, cmd.Flags().Lookup(FlagDeploymentIngressExposeLBHosts)); err != nil {
		return nil
	}

	cmd.Flags().Bool(FlagDeploymentNetworkPoliciesEnabled, true, "Enable network policies")
	if err := viper.BindPFlag(FlagDeploymentNetworkPoliciesEnabled, cmd.Flags().Lookup(FlagDeploymentNetworkPoliciesEnabled)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagDockerImagePullSecretsName, "", "Name of the local image pull secret configured with kubectl")
	if err := viper.BindPFlag(FlagDockerImagePullSecretsName, cmd.Flags().Lookup(FlagDockerImagePullSecretsName)); err != nil {
		return nil
	}

	cmd.Flags().Uint64(FlagOvercommitPercentMemory, 0, "Percentage of memory overcommit")
	if err := viper.BindPFlag(FlagOvercommitPercentMemory, cmd.Flags().Lookup(FlagOvercommitPercentMemory)); err != nil {
		return nil
	}

	cmd.Flags().Uint64(FlagOvercommitPercentCPU, 0, "Percentage of CPU overcommit")
	if err := viper.BindPFlag(FlagOvercommitPercentCPU, cmd.Flags().Lookup(FlagOvercommitPercentCPU)); err != nil {
		return nil
	}

	cmd.Flags().Uint64(FlagOvercommitPercentStorage, 0, "Percentage of storage overcommit")
	if err := viper.BindPFlag(FlagOvercommitPercentStorage, cmd.Flags().Lookup(FlagOvercommitPercentStorage)); err != nil {
		return nil
	}

	cmd.Flags().StringSlice(FlagDeploymentBlockedHostnames, nil, "hostnames blocked for deployments")
	if err := viper.BindPFlag(FlagDeploymentBlockedHostnames, cmd.Flags().Lookup(FlagDeploymentBlockedHostnames)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagAuthPem, "", "")

	if err := providerflags.AddKubeConfigPathFlag(cmd); err != nil {
		return nil
	}

	cmd.Flags().String(FlagDeploymentRuntimeClass, "gvisor", "kubernetes runtime class for deployments, use none for no specification")
	if err := viper.BindPFlag(FlagDeploymentRuntimeClass, cmd.Flags().Lookup(FlagDeploymentRuntimeClass)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagBidTimeout, 5*time.Minute, "time after which bids are cancelled if no lease is created")
	if err := viper.BindPFlag(FlagBidTimeout, cmd.Flags().Lookup(FlagBidTimeout)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagManifestTimeout, 5*time.Minute, "time after which bids are cancelled if no manifest is received")
	if err := viper.BindPFlag(FlagManifestTimeout, cmd.Flags().Lookup(FlagManifestTimeout)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagMetricsListener, "", "ip and port to start the metrics listener on")
	if err := viper.BindPFlag(FlagMetricsListener, cmd.Flags().Lookup(FlagMetricsListener)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagWithdrawalPeriod, time.Hour*24, "period at which withdrawals are made from the escrow accounts")
	if err := viper.BindPFlag(FlagWithdrawalPeriod, cmd.Flags().Lookup(FlagWithdrawalPeriod)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagLeaseFundsMonitorInterval, time.Minute*10, "interval at which lease is checked for funds available on the escrow accounts. >= 1m")
	if err := viper.BindPFlag(FlagLeaseFundsMonitorInterval, cmd.Flags().Lookup(FlagLeaseFundsMonitorInterval)); err != nil {
		return nil
	}

	cmd.Flags().Uint64(FlagMinimumBalance, mparams.DefaultBidMinDeposit.Amount.Mul(sdk.NewIntFromUint64(2)).Uint64(), "minimum account balance at which withdrawal is started")
	if err := viper.BindPFlag(FlagMinimumBalance, cmd.Flags().Lookup(FlagMinimumBalance)); err != nil {
		return nil
	}

	cmd.Flags().String(FlagProviderConfig, "", "provider configuration file path")
	if err := viper.BindPFlag(FlagProviderConfig, cmd.Flags().Lookup(FlagProviderConfig)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagRPCQueryTimeout, time.Minute, "timeout for requests made to the RPC node")
	if err := viper.BindPFlag(FlagRPCQueryTimeout, cmd.Flags().Lookup(FlagRPCQueryTimeout)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagCachedResultMaxAge, 5*time.Second, "max. cache age for results from the RPC node")
	if err := viper.BindPFlag(FlagCachedResultMaxAge, cmd.Flags().Lookup(FlagCachedResultMaxAge)); err != nil {
		return nil
	}

	cmd.Flags().Bool(FlagEnableIPOperator, false, "enable usage of the IP operator to lease IP addresses")
	if err := viper.BindPFlag(FlagEnableIPOperator, cmd.Flags().Lookup(FlagEnableIPOperator)); err != nil {
		return nil
	}

	cmd.Flags().Duration(FlagTxBroadcastTimeout, 30*time.Second, "tx broadcast timeout. defaults to 30s")
	if err := viper.BindPFlag(FlagTxBroadcastTimeout, cmd.Flags().Lookup(FlagTxBroadcastTimeout)); err != nil {
		return nil
	}

	if err := providerflags.AddServiceEndpointFlag(cmd, serviceHostnameOperator); err != nil {
		return nil
	}

	if err := providerflags.AddServiceEndpointFlag(cmd, serviceIPOperator); err != nil {
		return nil
	}

	return cmd
}

const (
	bidPricingStrategyScale       = "scale"
	bidPricingStrategyRandomRange = "randomRange"
	bidPricingStrategyShellScript = "shellScript"
)

var allowedBidPricingStrategies = [...]string{
	bidPricingStrategyScale,
	bidPricingStrategyRandomRange,
	bidPricingStrategyShellScript,
}

var errNoSuchBidPricingStrategy = fmt.Errorf("No such bid pricing strategy. Allowed: %v", allowedBidPricingStrategies)
var errInvalidValueForBidPrice = errors.New("not a valid bid price")
var errBidPriceNegative = errors.New("Bid price cannot be a negative number")

func strToBidPriceScale(val string) (decimal.Decimal, error) {
	v, err := decimal.NewFromString(val)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("%w: %s", errInvalidValueForBidPrice, val)
	}

	if v.IsNegative() {
		return decimal.Decimal{}, errBidPriceNegative
	}

	return v, nil
}

func createBidPricingStrategy(strategy string) (bidengine.BidPricingStrategy, error) {
	if strategy == bidPricingStrategyScale {
		cpuScale, err := strToBidPriceScale(viper.GetString(FlagBidPriceCPUScale))
		if err != nil {
			return nil, err
		}
		memoryScale, err := strToBidPriceScale(viper.GetString(FlagBidPriceMemoryScale))
		if err != nil {
			return nil, err
		}
		storageScale := make(bidengine.Storage)

		storageScales := strings.Split(viper.GetString(FlagBidPriceStorageScale), ",")
		for _, scalePair := range storageScales {
			vals := strings.Split(scalePair, "=")

			name := sdl.StorageEphemeral
			scaleVal := vals[0]

			if len(vals) == 2 {
				name = vals[0]
				scaleVal = vals[1]
			}

			storageScale[name], err = strToBidPriceScale(scaleVal)
			if err != nil {
				return nil, err
			}
		}

		endpointScale, err := strToBidPriceScale(viper.GetString(FlagBidPriceEndpointScale))
		if err != nil {
			return nil, err
		}

		ipScale, err := strToBidPriceScale(viper.GetString(FlagBidPriceIPScale))
		if err != nil {
			return nil, err
		}

		return bidengine.MakeScalePricing(cpuScale, memoryScale, storageScale, endpointScale, ipScale)
	}

	if strategy == bidPricingStrategyRandomRange {
		return bidengine.MakeRandomRangePricing()
	}

	if strategy == bidPricingStrategyShellScript {
		scriptPath := viper.GetString(FlagBidPriceScriptPath)
		processLimit := viper.GetUint(FlagBidPriceScriptProcessLimit)
		runtimeLimit := viper.GetDuration(FlagBidPriceScriptTimeout)
		return bidengine.MakeShellScriptPricing(scriptPath, processLimit, runtimeLimit)
	}

	return nil, errNoSuchBidPricingStrategy
}

// doRunCmd initializes all the Provider functionality, hangs, and awaits shutdown signals.
func doRunCmd(ctx context.Context, cmd *cobra.Command, _ []string) error {
	clusterPublicHostname := viper.GetString(FlagClusterPublicHostname)
	// TODO - validate that clusterPublicHostname is a valid hostname
	nodePortQuantity := viper.GetUint(FlagClusterNodePortQuantity)
	clusterWaitReadyDuration := viper.GetDuration(FlagClusterWaitReadyDuration)
	inventoryResourcePollPeriod := viper.GetDuration(FlagInventoryResourcePollPeriod)
	inventoryResourceDebugFreq := viper.GetUint(FlagInventoryResourceDebugFrequency)
	deploymentIngressStaticHosts := viper.GetBool(FlagDeploymentIngressStaticHosts)
	deploymentIngressDomain := viper.GetString(FlagDeploymentIngressDomain)
	deploymentNetworkPoliciesEnabled := viper.GetBool(FlagDeploymentNetworkPoliciesEnabled)
	dockerImagePullSecretsName := viper.GetString(FlagDockerImagePullSecretsName)
	strategy := viper.GetString(FlagBidPricingStrategy)
	deploymentIngressExposeLBHosts := viper.GetBool(FlagDeploymentIngressExposeLBHosts)
	// from := viper.GetString(flags.FlagFrom)
	overcommitPercentStorage := 1.0 + float64(viper.GetUint64(FlagOvercommitPercentStorage)/100.0)
	overcommitPercentCPU := 1.0 + float64(viper.GetUint64(FlagOvercommitPercentCPU)/100.0)
	// no GPU overcommit
	overcommitPercentGPU := 1.0
	overcommitPercentMemory := 1.0 + float64(viper.GetUint64(FlagOvercommitPercentMemory)/100.0)
	blockedHostnames := viper.GetStringSlice(FlagDeploymentBlockedHostnames)
	kubeConfigPath := viper.GetString(providerflags.FlagKubeConfig)
	deploymentRuntimeClass := viper.GetString(FlagDeploymentRuntimeClass)
	bidTimeout := viper.GetDuration(FlagBidTimeout)
	manifestTimeout := viper.GetDuration(FlagManifestTimeout)
	metricsListener := viper.GetString(FlagMetricsListener)
	providerConfig := viper.GetString(FlagProviderConfig)
	cachedResultMaxAge := viper.GetDuration(FlagCachedResultMaxAge)
	rpcQueryTimeout := viper.GetDuration(FlagRPCQueryTimeout)
	enableIPOperator := viper.GetBool(FlagEnableIPOperator)
	// txTimeout := viper.GetDuration(FlagTxBroadcastTimeout)

	pricing, err := createBidPricingStrategy(strategy)
	if err != nil {
		return err
	}

	logger := cmdutil.OpenLogger().With("cmp", "provider")
	kubeConfig, err := clientcommon.OpenKubeConfig(kubeConfigPath, logger)
	if err != nil {
		return err
	}

	var metricsRouter http.Handler
	if len(metricsListener) != 0 {
		metricsRouter = makeMetricsRouter()
	}

	clGroup, clCtx := errgroup.WithContext(ctx)

	cctx, err := sdkclient.GetClientTxContext(cmd)
	if err != nil {
		return err
	}

	cl, err := client.DiscoverClient(clCtx, cctx, cmd.Flags())
	if err != nil {
		return err
	}

	// keyname := cctx.GetFromName()
	// info, err := txFactory.Keybase().Key(keyname)
	// if err != nil {
	// 	return err
	// }

	gwaddr := viper.GetString(FlagGatewayListenAddress)

	var certFromFlag io.Reader
	if val := cmd.Flag(FlagAuthPem).Value.String(); val != "" {
		certFromFlag = bytes.NewBufferString(val)
	}

	kpm, err := cutils.NewKeyPairManager(cl.ClientContext(), cctx.FromAddress)
	if err != nil {
		return err
	}

	x509cert, tlsCert, err := kpm.ReadX509KeyPair(certFromFlag)
	if err != nil {
		return err
	}

	// Check that the certificate exists on chain and is not revoked
	cresp, err := cl.Query().Certificates(cmd.Context(), &ctypes.QueryCertificatesRequest{
		Filter: ctypes.CertificateFilter{
			Owner:  cctx.FromAddress.String(),
			Serial: x509cert.SerialNumber.String(),
			State:  "valid",
		},
	})
	if err != nil {
		return err
	}

	if len(cresp.Certificates) == 0 {
		return errors.Errorf("no valid found on chain certificate for account %s", cctx.FromAddress)
	}

	res, err := cl.Query().Provider(
		cmd.Context(),
		&ptypes.QueryProviderRequest{Owner: cctx.FromAddress.String()},
	)
	if err != nil {
		return err
	}

	pinfo := &res.Provider

	// k8s client creation
	kubeSettings := builder.NewDefaultSettings()
	kubeSettings.DeploymentIngressDomain = deploymentIngressDomain
	kubeSettings.DeploymentIngressExposeLBHosts = deploymentIngressExposeLBHosts
	kubeSettings.DeploymentIngressStaticHosts = deploymentIngressStaticHosts
	kubeSettings.NetworkPoliciesEnabled = deploymentNetworkPoliciesEnabled
	kubeSettings.ClusterPublicHostname = clusterPublicHostname
	kubeSettings.CPUCommitLevel = overcommitPercentCPU
	kubeSettings.GPUCommitLevel = overcommitPercentGPU
	kubeSettings.MemoryCommitLevel = overcommitPercentMemory
	kubeSettings.StorageCommitLevel = overcommitPercentStorage
	kubeSettings.DeploymentRuntimeClass = deploymentRuntimeClass
	kubeSettings.DockerImagePullSecretsName = strings.TrimSpace(dockerImagePullSecretsName)

	if err := builder.ValidateSettings(kubeSettings); err != nil {
		return err
	}

	clusterSettings := map[interface{}]interface{}{
		builder.SettingsKey: kubeSettings,
	}

	cclient, err := createClusterClient(cmd.Context(), logger, cmd, kubeConfigPath)
	if err != nil {
		return err
	}

	statusResult, err := cctx.Client.Status(cmd.Context())
	if err != nil {
		return err
	}
	currentBlockHeight := statusResult.SyncInfo.LatestBlockHeight
	session := session.New(logger, cl, pinfo, currentBlockHeight)

	if err := cctx.Client.Start(); err != nil {
		return err
	}

	bus := pubsub.NewBus()
	defer bus.Close()

	group, ctx := errgroup.WithContext(clCtx)

	// Provider service creation
	config := provider.NewDefaultConfig()
	config.ClusterWaitReadyDuration = clusterWaitReadyDuration
	config.ClusterPublicHostname = clusterPublicHostname
	config.ClusterExternalPortQuantity = nodePortQuantity
	config.InventoryResourceDebugFrequency = inventoryResourceDebugFreq
	config.InventoryResourcePollPeriod = inventoryResourcePollPeriod
	config.CPUCommitLevel = overcommitPercentCPU
	config.MemoryCommitLevel = overcommitPercentMemory
	config.StorageCommitLevel = overcommitPercentStorage
	config.BlockedHostnames = blockedHostnames
	config.DeploymentIngressStaticHosts = deploymentIngressStaticHosts
	config.DeploymentIngressDomain = deploymentIngressDomain
	config.BidTimeout = bidTimeout
	config.ManifestTimeout = manifestTimeout

	if len(providerConfig) != 0 {
		pConf, err := config2.ReadConfigPath(providerConfig)
		if err != nil {
			return err
		}
		config.Attributes = pConf.Attributes
		if err = config.Attributes.Validate(); err != nil {
			return err
		}
	}

	config.BalanceCheckerCfg = provider.BalanceCheckerConfig{
		WithdrawalPeriod:        viper.GetDuration(FlagWithdrawalPeriod),
		LeaseFundsCheckInterval: viper.GetDuration(FlagLeaseFundsMonitorInterval),
	}

	config.BidPricingStrategy = pricing
	config.ClusterSettings = clusterSettings

	bidDeposit, err := sdk.ParseCoinNormalized(viper.GetString(FlagBidDeposit))
	if err != nil {
		return err
	}
	config.BidDeposit = bidDeposit
	config.RPCQueryTimeout = rpcQueryTimeout
	config.CachedResultMaxAge = cachedResultMaxAge

	// This value can be nil, the operator is not mandatory
	var ipOperatorClient operatorclients.IPOperatorClient
	if enableIPOperator {
		endpoint, err := providerflags.GetServiceEndpointFlagValue(logger, serviceIPOperator)
		if err != nil {
			return err
		}
		ipOperatorClient, err = operatorclients.NewIPOperatorClient(logger, kubeConfig, endpoint)
		if err != nil {
			return err
		}
	}

	endpoint, err := providerflags.GetServiceEndpointFlagValue(logger, serviceHostnameOperator)
	if err != nil {
		return err
	}
	hostnameOperatorClient, err := operatorclients.NewHostnameOperatorClient(logger, kubeConfig, endpoint)
	if err != nil {
		return err
	}

	waitClients := make([]waiter.Waitable, 0)
	waitClients = append(waitClients, hostnameOperatorClient)

	if ipOperatorClient != nil {
		waitClients = append(waitClients, ipOperatorClient)
	}

	operatorWaiter := waiter.NewOperatorWaiter(cmd.Context(), logger, waitClients...)

	service, err := provider.NewService(ctx, cctx, cctx.FromAddress, session, bus, cclient, ipOperatorClient, operatorWaiter, config)
	if err != nil {
		return err
	}

	gateway, err := gwrest.NewServer(
		ctx,
		logger,
		service,
		cl.Query(),
		ipOperatorClient,
		gwaddr,
		cctx.FromAddress,
		[]tls.Certificate{tlsCert},
		clusterSettings,
	)
	if err != nil {
		return err
	}

	clGroup.Go(func() error {
		return group.Wait()
	})

	group.Go(func() error {
		return events.Publish(ctx, cctx.Client, "provider-cli", bus)
	})

	group.Go(func() error {
		<-service.Done()
		return nil
	})

	group.Go(func() error {
		// certificates are supplied via tls.Config
		return gateway.ListenAndServeTLS("", "")
	})

	group.Go(func() error {
		<-ctx.Done()
		return gateway.Close()
	})

	if metricsRouter != nil {
		group.Go(func() error {
			// fixme ovrclk/engineering#609
			// nolint: gosec
			srv := http.Server{Addr: metricsListener, Handler: metricsRouter}
			go func() {
				<-ctx.Done()
				_ = srv.Close()
			}()
			err := srv.ListenAndServe()
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		})
	}

	err = clGroup.Wait()

	if ipOperatorClient != nil {
		ipOperatorClient.Stop()
	}

	hostnameOperatorClient.Stop()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func createClusterClient(ctx context.Context, log log.Logger, _ *cobra.Command, configPath string) (cluster.Client, error) {
	if !viper.GetBool(FlagClusterK8s) {
		// Condition that there is no Kubernetes API to work with.
		return cluster.NullClient(), nil
	}
	ns := viper.GetString(providerflags.FlagK8sManifestNS)
	if ns == "" {
		return nil, fmt.Errorf("%w: --%s required", errInvalidConfig, providerflags.FlagK8sManifestNS)
	}
	return kube.NewClient(ctx, log, ns, configPath)
}

func showErrorToUser(err error) error {
	// If the error has a complete message associated with it then show it
	clientResponseError, ok := err.(gwrest.ClientResponseError)
	if ok && 0 != len(clientResponseError.Message) {
		_, _ = fmt.Fprintf(os.Stderr, "provider error messsage:\n%v\n", clientResponseError.Message)
		err = nil
	}

	return err
}
