// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostclient

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
)

// ProvisionedVcpusSetting is the number of estimated vCPUs that are available
// for the virtual cluster to use. This is independent of the number of nodes or
// vCPUs per node in the underlying host cluster, so that estimated CPU will not
// change when host cluster hardware changes. If zero, then the estimated CPU
// model will not be used by this tenant (e.g. because it's an on-demand
// tenant).
var ProvisionedVcpusSetting = settings.RegisterIntSetting(
	settings.SystemVisible,
	"tenant_cost_control.provisioned_vcpus",
	"number of vcpus available to the virtual cluster",
	0,
	settings.NonNegativeInt,
)

// InitialRequestSetting is exported for testing purposes.
var InitialRequestSetting = settings.RegisterFloatSetting(
	settings.SystemVisible,
	"tenant_cost_control.initial_request",
	"number of tokens to get from server on first request (requires restart)",
	bufferTokens/5,
	settings.FloatInRange(0, bufferTokens*10),
)

// TargetPeriodSetting is exported for testing purposes.
var TargetPeriodSetting = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"tenant_cost_control.token_request_period",
	"target duration between token bucket requests (requires restart)",
	10*time.Second,
	settings.DurationInRange(5*time.Second, 120*time.Second),
)

// CPUUsageAllowance is exported for testing purposes.
var CPUUsageAllowance = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"tenant_cost_control.cpu_usage_allowance",
	"this much CPU usage per second is considered background usage and "+
		"doesn't contribute to consumption; for example, if it is set to 10ms, "+
		"that corresponds to 1% of a CPU",
	10*time.Millisecond,
	settings.DurationInRange(0, 1000*time.Millisecond),
)

// defaultTickInterval is the default period at which we collect CPU usage and
// evaluate whether we need to send a new token request.
const defaultTickInterval = time.Second

// movingAvgTokensPerSecFactor is the weight applied to a new "sample" of token
// usage (with one sample per tickInterval).
//
// If we want a factor of 0.5 per second, this should be:
//
//	0.5^(1 second / tickInterval)
const movingAvgTokensPerSecFactor = 0.5

// movingAvgCPUPerSecFactor is the weight applied to a new sample of CPU usage.
const movingAvgCPUPerSecFactor = 0.5

// We request more tokens when the available tokens go below a threshold. The
// threshold is a fraction of the last granted tokens.
const notifyFraction = 0.1

// When we trickle tokens over a period of time, we request more tokens a bit
// before that period runs out. This "anticipation" should be more than what we
// expect the RTT of a token bucket request to be in practice.
const anticipation = time.Second

// If we have less than this many tokens to report, extend the reporting period
// to reduce load on the host cluster.
const consumptionReportingThreshold = 100

// The extended reporting period is this factor times the normal period.
const extendedReportingPeriodFactor = 4

// We try to maintain this many tokens in our local bucket, regardless of
// estimated usage. This is intended to support usage spikes without blocking.
const bufferTokens = 5000

// tokensPerCPUSecond is the factor used to convert from estimated KV CPU
// seconds to tokens in the distributed token bucket. This factor was chosen to
// convert from seconds to milliseconds, as that measurement has a similar
// magnitude as request units.
const tokensPerCPUSecond = 1000

// provisionedVcpusPerNode is the number of vCPUs per node, as used in the
// estimated CPU model. While virtual clusters do not have physical vCPUs or
// physical nodes, the model predicts CPU usage in a physical cluster, so assume
// that the modeled physical cluster has 8 vCPUs per node.
const provisionedVcpusPerNode = 8

func newTenantSideCostController(
	st *cluster.Settings,
	tenantID roachpb.TenantID,
	provider kvtenant.TokenBucketProvider,
	nodeDescs kvclient.NodeDescStore,
	locality roachpb.Locality,
	timeSource timeutil.TimeSource,
	testInstr TestInstrumentation,
) (*tenantSideCostController, error) {
	if tenantID == roachpb.SystemTenantID {
		return nil, errors.AssertionFailedf("cost controller can't be used for system tenant")
	}
	c := &tenantSideCostController{
		timeSource:          timeSource,
		testInstr:           testInstr,
		settings:            st,
		tenantID:            tenantID,
		provider:            provider,
		nodeDescs:           nodeDescs,
		locality:            locality,
		responseChan:        make(chan *kvpb.TokenBucketResponse, 1),
		lowTokensNotifyChan: make(chan struct{}, 1),
	}

	// Initialize metrics.
	c.metrics.Init(locality)

	// Start with filled burst buffer.
	c.limiter.Init(&c.metrics, timeSource, c.lowTokensNotifyChan)
	c.limiter.Reconfigure(timeSource.Now(), limiterReconfigureArgs{
		NewTokens:       bufferTokens,
		NotifyThreshold: bufferTokens,
	})

	storeModels := func() {
		ruModel := tenantcostmodel.RequestUnitModelFromSettings(&st.SV)
		c.ruModel.Store(&ruModel)

		cpuModel := tenantcostmodel.EstimatedCPUModelFromSettings(&st.SV)
		c.cpuModel.Store(&cpuModel)
	}

	// If any of the cost settings change, reload both models.
	tenantcostmodel.SetOnChange(&st.SV, func(ctx context.Context) {
		storeModels()
	})
	storeModels()

	vcpusChanged := func() {
		vcpus := ProvisionedVcpusSetting.Get(&st.SV)
		var nodeCount float64
		if vcpus > 0 {
			// Divide the number of vCPUs in the virtual cluster by 8 to derive
			// the number of nodes to use when estimated CPU (min 3 nodes).
			nodeCount = math.Max(float64(vcpus)/provisionedVcpusPerNode, 3)
		}
		c.provisionedNodes.Store(math.Float64bits(nodeCount))

		// Update the metric to reflect the new number of provisioned vCPUs.
		c.metrics.ProvisionedVcpus.Update(vcpus)
	}

	ProvisionedVcpusSetting.SetOnChange(&st.SV, func(ctx context.Context) {
		vcpusChanged()
	})
	vcpusChanged()

	return c, nil
}

// NewTenantSideCostController creates an object which implements the
// server.TenantSideCostController interface.
func NewTenantSideCostController(
	st *cluster.Settings,
	tenantID roachpb.TenantID,
	provider kvtenant.TokenBucketProvider,
	nodeDescs kvclient.NodeDescStore,
	locality roachpb.Locality,
) (multitenant.TenantSideCostController, error) {
	return newTenantSideCostController(
		st, tenantID, provider, nodeDescs, locality,
		timeutil.DefaultTimeSource{},
		nil, /* testInstr */
	)
}

// TestingTenantSideCostController is a testing variant of
// NewTenantSideCostController which allows using a specified TimeSource.
func TestingTenantSideCostController(
	st *cluster.Settings,
	tenantID roachpb.TenantID,
	provider kvtenant.TokenBucketProvider,
	nodeDescs kvclient.NodeDescStore,
	locality roachpb.Locality,
	timeSource timeutil.TimeSource,
	testInstr TestInstrumentation,
) (multitenant.TenantSideCostController, error) {
	return newTenantSideCostController(st, tenantID, provider, nodeDescs, locality, timeSource, testInstr)
}

// TestingTokenBucketString returns a string representation of the tenant's
// token bucket, for testing purposes.
func TestingTokenBucketString(ctrl multitenant.TenantSideCostController) string {
	c := ctrl.(*tenantSideCostController)
	return c.limiter.String(c.timeSource.Now())
}

// TestingAvailableTokens returns the current number of available tokens in the
// tenant's token bucket, for testing purposes.
func TestingAvailableTokens(ctrl multitenant.TenantSideCostController) float64 {
	c := ctrl.(*tenantSideCostController)
	return c.limiter.AvailableTokens(c.timeSource.Now())
}

// TestingSetRate sets the fill rate of the tenant's token bucket, for testing
// purposes.
func TestingSetRate(ctrl multitenant.TenantSideCostController, rate float64) {
	c := ctrl.(*tenantSideCostController)
	c.limiter.Reconfigure(c.timeSource.Now(), limiterReconfigureArgs{NewRate: rate})
}

func init() {
	server.NewTenantSideCostController = NewTenantSideCostController
}

type tenantSideCostController struct {
	metrics              metrics
	timeSource           timeutil.TimeSource
	testInstr            TestInstrumentation
	settings             *cluster.Settings
	ruModel              atomic.Pointer[tenantcostmodel.RequestUnitModel]
	cpuModel             atomic.Pointer[tenantcostmodel.EstimatedCPUModel]
	tenantID             roachpb.TenantID
	provider             kvtenant.TokenBucketProvider
	nodeDescs            kvclient.NodeDescStore
	locality             roachpb.Locality
	limiter              limiter
	stopper              *stop.Stopper
	instanceID           base.SQLInstanceID
	sessionID            sqlliveness.SessionID
	externalUsageFn      multitenant.ExternalUsageFn
	nextLiveInstanceIDFn multitenant.NextLiveInstanceIDFn

	// avgSQLCPUPerSec is an exponentially-weighted moving average of the SQL CPU
	// usage per second; used to estimate the CPU usage of a query. It is only
	// written in the main loop, but can be read by multiple goroutines so is an
	// atomic. It is a Uint64-encoded float64 value.
	avgSQLCPUPerSec atomic.Uint64

	// globalWriteQPS is the recent global rate of write batches per second for this
	// tenant, measured across all SQL pods. It is only written in the main loop,
	// but can be read by multiple goroutines so is an atomic. It is a
	// Uint64-encoded float64 value.
	globalWriteQPS atomic.Uint64

	// lowTokensNotifyChan is used when the number of available tokens is running
	// low and we need to send an early token bucket request.
	lowTokensNotifyChan chan struct{}

	// responseChan is used to receive results from token bucket requests, which
	// are run in a separate goroutine. A nil response indicates an error.
	responseChan chan *kvpb.TokenBucketResponse

	// provisionedNodes is the number of nodes assumed to be in the virtual
	// cluster, for the purpose of calculating estimated CPU. It is calculated
	// from the estimated_vcpus setting. It is a Uint64-encoded float64 value and
	// is always 0 for the RU model.
	provisionedNodes atomic.Uint64

	// run contains the state that is updated by the main loop. It doesn't need a
	// mutex since the main loop runs on a single goroutine.
	run struct {
		// lastTick is the time recorded when the last tick was received (one
		// tick per second by default).
		lastTick time.Time
		// externalUsage stores the last value returned by externalUsageFn.
		externalUsage multitenant.ExternalUsage
		// tickTokens stores the total tokens consumed as of the last tick.
		tickTokens float64
		// tickBatches stores the total batches consumed as of the last tick.
		tickBatches int64
		// tickEstimatedCPUSecs stores the total estimated CPU consumed as of the
		// last tick.
		tickEstimatedCPUSecs float64
		// targetPeriod stores the value of the TargetPeriodSetting setting at the
		// last update.
		targetPeriod time.Duration

		// requestSeqNum is an increasing sequence number that is included in token
		// bucket requests to prevent duplicate consumption reporting.
		requestSeqNum int64
		// requestInProgress is the token bucket request that is in progress, or
		// nil if there is no call in progress. It gets set to nil when we process
		// the response (in the main loop), even in error cases.
		requestInProgress *kvpb.TokenBucketRequest
		// shouldSendRequest is set if the last token bucket request encountered an
		// error. This triggers a retry attempt on the next tick.
		//
		// Note: shouldSendRequest should be true only when requestInProgress is
		// not nil.
		shouldSendRequest bool

		// lastRequestTime is the time that the last token bucket request was
		// sent to the server.
		lastRequestTime time.Time
		// lastReportedTokens is the total number of consumed tokens as of the
		// last report to the token bucket server.
		lastReportedTokens float64
		// lastReportedConsumption is the set of tenant resource consumption
		// metrics last sent to the token bucket server.
		lastReportedConsumption kvpb.TenantConsumption
		// lastRate is the token bucket fill rate that was last configured.
		lastRate float64
		// globalEstimatedCPURate is the global rate of estimated CPU usage, in
		// CPU-seconds, as last reported by the token bucket server.
		globalEstimatedCPURate float64

		// When we obtain tokens that are throttled over a period of time, we
		// will request more only when we get close to the end of that trickle.
		// trickleTimer will send an event on trickleCh when we get close.
		trickleTimer timeutil.TimerI
		trickleCh    <-chan time.Time
		// trickleDeadline specifies the time at which trickle tokens granted by
		// the token bucket server will be fully added to the local token bucket.
		// If the server directly granted tokens with no trickle deadline, then
		// this is zero-valued.
		trickleDeadline time.Time

		// fallbackRate is the refill rate we fall back to if the token bucket
		// requests don't complete or take a long time.
		fallbackRate float64
		// fallbackRateStart is the time when we can switch to the fallback rate;
		// set only when we get a low tokens notification. It is cleared when we
		// get a successful token bucket response, so it only takes effect if the
		// token bucket server is unavailable or slow.
		fallbackRateStart time.Time

		// avgTokensPerSec is an exponentially-weighted moving average of tokens
		// consumed per second. It is used to estimate the token requirements for
		// the next request.
		avgTokensPerSec float64
	}
}

var _ multitenant.TenantSideCostController = (*tenantSideCostController)(nil)

// Start is part of multitenant.TenantSideCostController.
func (c *tenantSideCostController) Start(
	ctx context.Context,
	stopper *stop.Stopper,
	instanceID base.SQLInstanceID,
	sessionID sqlliveness.SessionID,
	externalUsageFn multitenant.ExternalUsageFn,
	nextLiveInstanceIDFn multitenant.NextLiveInstanceIDFn,
) error {
	if instanceID == 0 {
		return errors.New("invalid SQLInstanceID")
	}
	if sessionID == "" {
		return errors.New("invalid sqlliveness.SessionID")
	}
	c.stopper = stopper
	c.instanceID = instanceID
	c.sessionID = sessionID
	c.externalUsageFn = externalUsageFn
	c.nextLiveInstanceIDFn = nextLiveInstanceIDFn

	return stopper.RunAsyncTask(ctx, "cost-controller", func(ctx context.Context) {
		c.mainLoop(ctx)
	})
}

func (c *tenantSideCostController) useRequestUnitModel() bool {
	return c.provisionedNodes.Load() == 0
}

func (c *tenantSideCostController) initRunState(ctx context.Context) {
	c.run.targetPeriod = TargetPeriodSetting.Get(&c.settings.SV)

	now := c.timeSource.Now()
	c.run.lastTick = now
	c.run.externalUsage = c.externalUsageFn(ctx)
	c.run.lastRequestTime = now
	c.run.avgTokensPerSec = InitialRequestSetting.Get(&c.settings.SV) / c.run.targetPeriod.Seconds()
	c.run.requestSeqNum = 1
}

// onTick is called whenever the main loop awakens, in order to account for CPU
// and Egress usage in the interim.
func (c *tenantSideCostController) onTick(ctx context.Context, newTime time.Time) {
	newExternalUsage := c.externalUsageFn(ctx)

	// Update CPU consumption.
	deltaCPUSecs := newExternalUsage.CPUSecs - c.run.externalUsage.CPUSecs
	var totalBatches = c.metrics.TotalReadBatches.Count() + c.metrics.TotalWriteBatches.Count()

	deltaTime := newTime.Sub(c.run.lastTick)
	if deltaTime > 0 {
		// Subtract any allowance that we consider free background usage.
		allowance := CPUUsageAllowance.Get(&c.settings.SV).Seconds() * deltaTime.Seconds()
		deltaCPUSecs -= allowance

		// If total CPU usage is small (less than 3% of a single CPU by default)
		// and there have been no recent read/write operations, then ignore the
		// recent usage altogether. This is intended to minimize reported usage
		// when the cluster is idle.
		if deltaCPUSecs < allowance*2 {
			if totalBatches == c.run.tickBatches {
				// There have been no batches since the last tick.
				deltaCPUSecs = 0
			}
		}

		// Keep track of an exponential moving average of CPU usage.
		avgCPU := deltaCPUSecs / deltaTime.Seconds()
		avgCPUPerSec := math.Float64frombits(c.avgSQLCPUPerSec.Load())
		avgCPUPerSec *= 1 - movingAvgCPUPerSecFactor
		avgCPUPerSec += avgCPU * movingAvgCPUPerSecFactor
		c.avgSQLCPUPerSec.Store(math.Float64bits(avgCPUPerSec))
	}
	if deltaCPUSecs < 0 {
		deltaCPUSecs = 0
	}

	var deltaPGWireEgressBytes int64
	if newExternalUsage.PGWireEgressBytes > c.run.externalUsage.PGWireEgressBytes {
		deltaPGWireEgressBytes = int64(newExternalUsage.PGWireEgressBytes - c.run.externalUsage.PGWireEgressBytes)
		c.metrics.TotalPGWireEgressBytes.Inc(deltaPGWireEgressBytes)
	}

	// KV RUs and estimated KV CPU are not included here, since the CPU metric
	// corresponds only to the SQL layer.
	c.metrics.TotalSQLPodsCPUSeconds.Inc(deltaCPUSecs)

	var newTokens, totalTokens float64
	if c.useRequestUnitModel() {
		// Compute consumed RU.
		ruModel := c.ruModel.Load()
		newTokens = float64(ruModel.PodCPUCost(deltaCPUSecs))
		newTokens += float64(ruModel.PGWireEgressCost(deltaPGWireEgressBytes))
		c.metrics.TotalRU.Inc(newTokens)
		totalTokens = c.metrics.TotalRU.Count()
	} else {
		// Account for background CPU usage.
		cpuModel := c.cpuModel.Load()

		// Determine how much eCPU was consumed by this SQL node during the
		// last tick.
		totalEstimatedCPUSecs := c.metrics.TotalEstimatedCPUSeconds.Count() + deltaCPUSecs
		deltaEstimatedCPUSecs := totalEstimatedCPUSecs - c.run.tickEstimatedCPUSecs
		if deltaEstimatedCPUSecs > 0 && cpuModel.BackgroundCPU.Amortization > 0 {
			deltaCPUSecs += calculateBackgroundCPUSecs(
				cpuModel, c.run.globalEstimatedCPURate, deltaTime, deltaEstimatedCPUSecs)
		}

		// Remember estimated CPU for next time. Include background CPU so that it
		// won't be included in the background CPU calculation for the next tick
		// (i.e. we don't want background CPU to be recursively calculated on itself).
		c.run.tickEstimatedCPUSecs = totalEstimatedCPUSecs + deltaCPUSecs

		// Convert CPU into tokens in order to adjust the token bucket.
		c.metrics.TotalEstimatedCPUSeconds.Inc(deltaCPUSecs)
		newTokens = deltaCPUSecs * tokensPerCPUSecond
		totalTokens = c.metrics.TotalEstimatedCPUSeconds.Count() * tokensPerCPUSecond
	}

	// Update the average newTokens consumed per second, based on the latest stats.
	delta := totalTokens - c.run.tickTokens
	avg := delta * float64(time.Second) / float64(deltaTime)
	c.run.avgTokensPerSec = movingAvgTokensPerSecFactor*avg + (1-movingAvgTokensPerSecFactor)*c.run.avgTokensPerSec

	c.run.lastTick = newTime
	c.run.externalUsage = newExternalUsage
	c.run.tickTokens = totalTokens
	c.run.tickBatches = totalBatches

	// Remove the new tokens from the bucket.
	c.limiter.RemoveTokens(newTime, newTokens)

	// Switch to the fallback rate if needed.
	if !c.run.fallbackRateStart.IsZero() && !newTime.Before(c.run.fallbackRateStart) &&
		c.run.fallbackRate != 0 {
		log.Infof(ctx, "switching to fallback rate %.10g tokens/s", c.run.fallbackRate)
		c.limiter.Reconfigure(c.timeSource.Now(), limiterReconfigureArgs{
			NewRate:   c.run.fallbackRate,
			MaxTokens: bufferTokens + c.run.fallbackRate*c.run.targetPeriod.Seconds(),
		})
		c.run.fallbackRateStart = time.Time{}
	}

	// Should a token bucket request be sent? It might be for a retry or for
	// periodic consumption reporting.
	if c.run.shouldSendRequest || c.shouldReportConsumption() {
		c.sendTokenBucketRequest(ctx)
	}
}

// shouldReportConsumption decides if it's time to send a token bucket request
// to report consumption.
func (c *tenantSideCostController) shouldReportConsumption() bool {
	timeSinceLastRequest := c.run.lastTick.Sub(c.run.lastRequestTime)
	if timeSinceLastRequest >= c.run.targetPeriod {
		consumptionToReport := c.run.tickTokens - c.run.lastReportedTokens
		if consumptionToReport >= consumptionReportingThreshold {
			return true
		}
		if timeSinceLastRequest >= extendedReportingPeriodFactor*c.run.targetPeriod {
			return true
		}
	}

	return false
}

func (c *tenantSideCostController) sendTokenBucketRequest(ctx context.Context) {
	if c.run.requestInProgress != nil {
		// Don't allow multiple concurrent token bucket requests. But do send
		// another request once the in-progress request completes.
		c.run.shouldSendRequest = true
		return
	}
	c.run.shouldSendRequest = false

	// Compute consumption delta since last report to the server.
	var latestConsumption, deltaConsumption kvpb.TenantConsumption
	c.metrics.getConsumption(&latestConsumption)
	deltaConsumption = latestConsumption
	deltaConsumption.Sub(&c.run.lastReportedConsumption)

	var requested float64
	now := c.timeSource.Now()

	if c.run.trickleTimer != nil {
		// Don't request additional tokens if we're in the middle of a trickle
		// that was started recently.
		requested = 0
	} else {
		// Request what we expect to need over the next target period plus the
		// buffer amount.
		requested = c.run.avgTokensPerSec*c.run.targetPeriod.Seconds() + bufferTokens

		// Requested tokens are adjusted by what's still left in the burst buffer.
		// Note that this can be negative, which indicates we are in debt. In that
		// case, enough tokens should be added to the request to cover the debt.
		requested -= c.limiter.AvailableTokens(now)
		if requested < 0 {
			// We don't need more tokens right now, but we still want to report
			// consumption.
			requested = 0
		}

		// Switch to fallback rate if the response takes too long.
		c.run.fallbackRateStart = now.Add(anticipation)
	}

	req := &kvpb.TokenBucketRequest{
		TenantID:                    c.tenantID.ToUint64(),
		InstanceID:                  uint32(c.instanceID),
		InstanceLease:               c.sessionID.UnsafeBytes(),
		NextLiveInstanceID:          uint32(c.nextLiveInstanceIDFn(ctx)),
		SeqNum:                      c.run.requestSeqNum,
		ConsumptionSinceLastRequest: deltaConsumption,
		ConsumptionPeriod:           now.Sub(c.run.lastRequestTime),
		RequestedTokens:             requested,
		TargetRequestPeriod:         c.run.targetPeriod,
	}
	c.run.requestInProgress = req
	c.run.requestSeqNum++

	c.run.lastRequestTime = now
	c.run.lastReportedConsumption = latestConsumption

	if c.useRequestUnitModel() {
		c.run.lastReportedTokens = latestConsumption.RU
	} else {
		c.run.lastReportedTokens = c.metrics.TotalEstimatedCPUSeconds.Count() * tokensPerCPUSecond
	}

	ctx, _ = c.stopper.WithCancelOnQuiesce(ctx)
	err := c.stopper.RunAsyncTask(ctx, "token-bucket-request", func(ctx context.Context) {
		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Infof(ctx, "TokenBucket request: %s\n", req.String())
		}
		resp, err := c.provider.TokenBucket(ctx, req)
		if err != nil {
			// Don't log any errors caused by the stopper canceling the context.
			if !errors.Is(err, context.Canceled) {
				log.Warningf(ctx, "TokenBucket RPC error: %v", err)
			}
			resp = nil
		} else if (resp.Error != errorspb.EncodedError{}) {
			// This is a "logic" error which indicates a configuration problem on the
			// host side. We will keep retrying periodically.
			err = errors.DecodeError(ctx, resp.Error)
			log.Warningf(ctx, "TokenBucket error: %v", err)
			resp = nil
		}
		c.responseChan <- resp
	})
	if err != nil {
		// We are shutting down and could not send the request.
		c.responseChan <- nil
	}
}

func (c *tenantSideCostController) handleTokenBucketResponse(
	ctx context.Context, req *kvpb.TokenBucketRequest, resp *kvpb.TokenBucketResponse,
) {
	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Infof(
			ctx, "TokenBucket response: %g tokens over %s (fallback rate %g)",
			resp.GrantedTokens, resp.TrickleDuration, resp.FallbackRate,
		)
	}

	// Reset fallback rate now that we've gotten a response.
	c.run.fallbackRate = resp.FallbackRate
	c.run.fallbackRateStart = time.Time{}

	// Process granted tokens.
	now := c.timeSource.Now()
	granted := resp.GrantedTokens

	// Shut down any trickle previously in-progress trickle.
	if c.run.trickleTimer != nil {
		c.run.trickleTimer.Stop()
		c.run.trickleTimer = nil
		c.run.trickleCh = nil
	}
	if !c.run.trickleDeadline.IsZero() {
		// If last request came with a trickle duration, we may have tokens that
		// were not made available to the bucket yet; throw them together with the
		// newly granted tokens.
		// NB: There is a race condition here, where the token bucket can consume
		// tokens between the time we call Now() and the time we reconfigure the
		// bucket below. This would result in double usage of the same granted
		// tokens. However, this is not a big concern, since it's a small window,
		// and even if it occurs, the usage is still counted. The only effect is
		// some extra debt accumulation, which is fine.
		if since := c.run.trickleDeadline.Sub(now); since > 0 {
			granted += c.run.lastRate * since.Seconds()
		}
		c.run.trickleDeadline = time.Time{}
	}

	var cfg limiterReconfigureArgs

	// Directly add tokens to the bucket if they're immediately available.
	// Configure a token trickle if the tokens are only available over time.
	if resp.TrickleDuration == 0 {
		// Calculate the threshold at which a low tokens notification will be sent.
		notifyThreshold := granted * notifyFraction
		if notifyThreshold < bufferTokens {
			notifyThreshold = bufferTokens
		}

		// We received a batch of tokens to use as needed. Set up the token
		// bucket to notify us when the tokens are running low.
		cfg.NewTokens = granted
		cfg.NewRate = 0

		// Configure the low tokens notification threshold. However, if the server
		// could not even grant the tokens that were requested, then avoid triggering
		// extra calls to the server. The next call to the server will be made by
		// the next regularly scheduled consumption reporting interval.
		if req.RequestedTokens == resp.GrantedTokens {
			cfg.NotifyThreshold = notifyThreshold
		}
	} else {
		// We received a batch of tokens that can only be used over the
		// TrickleDuration. Set up the token bucket to notify us a bit before
		// this period elapses.
		timerDuration := resp.TrickleDuration - anticipation
		if timerDuration <= 0 {
			timerDuration = (resp.TrickleDuration + 1) / 2
		}
		c.run.trickleTimer = c.timeSource.NewTimer()
		c.run.trickleTimer.Reset(timerDuration)
		c.run.trickleCh = c.run.trickleTimer.Ch()
		c.run.trickleDeadline = now.Add(resp.TrickleDuration)

		cfg.NewRate = granted / resp.TrickleDuration.Seconds()
		cfg.MaxTokens = bufferTokens + granted
	}

	if !c.useRequestUnitModel() {
		// Store global write QPS for the tenant.
		c.globalWriteQPS.Store(math.Float64bits(resp.ConsumptionRates.WriteBatchRate))
		c.run.globalEstimatedCPURate = resp.ConsumptionRates.EstimatedCPURate
	}

	c.limiter.Reconfigure(now, cfg)
	c.run.lastRate = cfg.NewRate

	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Infof(ctx, "Limiter: %s", c.limiter.String(now))
	}
}

func (c *tenantSideCostController) mainLoop(ctx context.Context) {
	tickInterval := defaultTickInterval
	// Make sure the tick interval is never larger than the target request period.
	// This is useful for tests which set a very small period.
	if targetPeriod := TargetPeriodSetting.Get(&c.settings.SV); targetPeriod < tickInterval {
		tickInterval = targetPeriod
	}
	ticker := c.timeSource.NewTicker(tickInterval)
	defer ticker.Stop()
	tickerCh := ticker.Ch()

	c.initRunState(ctx)

	if c.testInstr != nil {
		c.testInstr.Event(c.timeSource.Now(), MainLoopStarted)
	}

	// The main loop should never block. The remote requests run in separate
	// goroutines.
	for {
		select {
		case <-tickerCh:
			// If ticks are delayed, or we're slow in receiving, they can get backed
			// up. Discard any ticks which are received too quickly in succession.
			// Note that we're deliberately not using the time received from tickerCh
			// because it might cause logic bugs when developers assume it is
			// consistent with timeSource.Now (e.g. by computing extreme averages
			// due to tiny intervals between ticks).
			now := c.timeSource.Now()
			if now.Before(c.run.lastTick.Add(tickInterval / 2)) {
				break
			}

			c.onTick(ctx, now)
			if c.testInstr != nil {
				c.testInstr.Event(now, TickProcessed)
			}

		case resp := <-c.responseChan:
			req := c.run.requestInProgress
			c.run.requestInProgress = nil
			if resp != nil {
				// Token bucket request was successful.
				c.handleTokenBucketResponse(ctx, req, resp)

				// Immediately send another token bucket request if one was requested
				// while this one was in progress.
				if c.run.shouldSendRequest {
					c.sendTokenBucketRequest(ctx)
				}

				if c.testInstr != nil {
					c.testInstr.Event(c.timeSource.Now(), TokenBucketResponseProcessed)
				}
			} else {
				// A nil response indicates a failure (which would have been logged).
				// Retry the request on the next tick so there's at least some
				// delay between retries.
				c.run.shouldSendRequest = true

				if c.testInstr != nil {
					c.testInstr.Event(c.timeSource.Now(), TokenBucketResponseError)
				}
			}

		case <-c.run.trickleCh:
			// Trickle is about to end, so configure the low tokens notification
			// so that another token bucket request will be triggered if/when the
			// bucket gets low (or is already low).
			c.run.trickleTimer = nil
			c.run.trickleCh = nil
			c.sendTokenBucketRequest(ctx)

		case <-c.lowTokensNotifyChan:
			// Switch to fallback rate if we don't get a token bucket response
			// soon enough.
			c.sendTokenBucketRequest(ctx)

			if c.testInstr != nil {
				c.testInstr.Event(c.timeSource.Now(), LowTokensNotification)
			}

		case <-c.stopper.ShouldQuiesce():
			c.limiter.Close()
			// TODO(radu): send one last request to update consumption.
			return
		}
	}
}

// OnRequestWait is part of the multitenant.TenantSideKVInterceptor interface.
func (c *tenantSideCostController) OnRequestWait(ctx context.Context) error {
	if multitenant.HasTenantCostControlExemption(ctx) {
		return nil
	}

	// Note that the tenantSideController might not be started yet; that is ok
	// because we initialize the limiter with some initial tokens and a reasonable
	// initial rate.
	return c.limiter.Wait(ctx, 0)
}

// OnResponseWait is part of the multitenant.TenantSideBatchInterceptor
// interface.
func (c *tenantSideCostController) OnResponseWait(
	ctx context.Context,
	request *kvpb.BatchRequest,
	response *kvpb.BatchResponse,
	targetRange *roachpb.RangeDescriptor,
	targetReplica *roachpb.ReplicaDescriptor,
) error {
	if multitenant.HasTenantCostControlExemption(ctx) {
		return nil
	}

	// Account for the cost of write requests and read responses.
	var batchInfo tenantcostmodel.BatchInfo
	var tokens float64
	replicas := int64(len(targetRange.Replicas().Descriptors()))
	nodeCount := math.Float64frombits(c.provisionedNodes.Load())
	if nodeCount == 0 {
		// Calculate RU consumption for the operation.
		ruModel := c.ruModel.Load()
		batchInfo = ruModel.MakeBatchInfo(request, response)

		networkCost := c.computeNetworkCost(ctx, targetRange, targetReplica, batchInfo.WriteCount > 0)
		kvRU, networkRU := ruModel.BatchCost(batchInfo, networkCost, replicas)
		totalRU := kvRU + networkRU
		tokens = float64(totalRU)

		c.metrics.TotalKVRU.Inc(float64(kvRU))
		c.metrics.TotalRU.Inc(float64(totalRU))
		c.metrics.TotalCrossRegionNetworkRU.Inc(float64(networkRU))

		// Record the number of RUs consumed by the IO request.
		if execinfra.IncludeRUEstimateInExplainAnalyze.Get(&c.settings.SV) {
			if sp := tracing.SpanFromContext(ctx); sp != nil &&
				sp.RecordingType() != tracingpb.RecordingOff {
				sp.RecordStructured(&kvpb.TenantConsumption{
					RU: float64(totalRU),
				})
			}
		}
	} else {
		// Estimate CPU usage for the operation.
		cpuModel := c.cpuModel.Load()
		batchInfo = cpuModel.MakeBatchInfo(request, response)
		writeQPS := math.Float64frombits(c.globalWriteQPS.Load())
		estimatedCPU := cpuModel.BatchCost(batchInfo, writeQPS/nodeCount, replicas)
		tokens = float64(estimatedCPU) * tokensPerCPUSecond

		c.metrics.TotalEstimatedKVCPUSeconds.Inc(float64(estimatedCPU))
		c.metrics.TotalEstimatedCPUSeconds.Inc(float64(estimatedCPU))

		if batchInfo.WriteBytes > 0 {
			// Increment metrics that track estimated number of bytes that will
			// be replicated from the leaseholder to other replicas in the range.
			c.UpdateEstimatedWriteReplicationBytes(ctx, targetRange, targetReplica, batchInfo.WriteBytes)
		}
	}

	if batchInfo.ReadCount > 0 {
		c.metrics.TotalReadBatches.Inc(1)
		c.metrics.TotalReadRequests.Inc(batchInfo.ReadCount)
		c.metrics.TotalReadBytes.Inc(batchInfo.ReadBytes)
	}
	if batchInfo.WriteCount > 0 {
		c.metrics.TotalWriteBatches.Inc(replicas)
		c.metrics.TotalWriteRequests.Inc(replicas * batchInfo.WriteCount)
		c.metrics.TotalWriteBytes.Inc(replicas * batchInfo.WriteBytes)
	}

	// TODO(andyk): Consider breaking up huge acquisition requests into chunks
	// that can be fulfilled separately and reported separately. This would make
	// it easier to stick within a constrained tokens/s budget.
	if err := c.limiter.Wait(ctx, tokens); err != nil {
		return err
	}

	return nil
}

// OnExternalIOWait is part of the multitenant.TenantSideExternalIORecorder
// interface.
func (c *tenantSideCostController) OnExternalIOWait(
	ctx context.Context, usage multitenant.ExternalIOUsage,
) error {
	return c.onExternalIO(ctx, usage, true /* wait */)
}

// OnExternalIO is part of the multitenant.TenantSideExternalIORecorder
// interface. TODO(drewk): collect this for queries.
func (c *tenantSideCostController) OnExternalIO(
	ctx context.Context, usage multitenant.ExternalIOUsage,
) {
	// No error possible if not waiting.
	_ = c.onExternalIO(ctx, usage, false /* wait */)
}

// onExternalIO records external I/O usage, optionally waiting until there are
// sufficient tokens in the bucket. This can fail if wait=true and the wait is
// canceled.
func (c *tenantSideCostController) onExternalIO(
	ctx context.Context, usage multitenant.ExternalIOUsage, wait bool,
) error {
	if multitenant.HasTenantCostControlExemption(ctx) {
		return nil
	}

	c.metrics.TotalExternalIOIngressBytes.Inc(usage.IngressBytes)
	c.metrics.TotalExternalIOEgressBytes.Inc(usage.EgressBytes)

	if c.useRequestUnitModel() {
		costCfg := c.ruModel.Load()
		totalRU := costCfg.ExternalIOIngressCost(usage.IngressBytes) +
			costCfg.ExternalIOEgressCost(usage.EgressBytes)
		c.metrics.TotalRU.Inc(float64(totalRU))

		if wait {
			if err := c.limiter.Wait(ctx, float64(totalRU)); err != nil {
				return err
			}
		} else {
			c.limiter.RemoveTokens(c.timeSource.Now(), float64(totalRU))
		}
	}

	return nil
}

// GetCPUMovingAvg is used to obtain an exponential moving average estimate
// for the CPU usage in seconds per each second of wall-clock time.
func (c *tenantSideCostController) GetCPUMovingAvg() float64 {
	return math.Float64frombits(c.avgSQLCPUPerSec.Load())
}

// GetRequestUnitModel is part of the multitenant.TenantSideCostController
// interface.
func (c *tenantSideCostController) GetRequestUnitModel() *tenantcostmodel.RequestUnitModel {
	return c.ruModel.Load()
}

// GetEstimatedCPUModel is part of the multitenant.TenantSideCostController
// interface.
func (c *tenantSideCostController) GetEstimatedCPUModel() *tenantcostmodel.EstimatedCPUModel {
	return c.cpuModel.Load()
}

// Metrics returns a metric.Struct which holds metrics for the controller.
func (c *tenantSideCostController) Metrics() metric.Struct {
	return &c.metrics
}

// computeNetworkCost calculates the network cost multiplier for a read or
// write operation. The network cost accounts for the logical byte traffic
// between the client region and the replica regions.
func (c *tenantSideCostController) computeNetworkCost(
	ctx context.Context,
	targetRange *roachpb.RangeDescriptor,
	targetReplica *roachpb.ReplicaDescriptor,
	isWrite bool,
) tenantcostmodel.NetworkCost {
	// It is unfortunate that we hardcode a particular locality tier name here.
	// Ideally, we would have a cluster setting that specifies the name or some
	// other way to configure it.
	clientRegion, _ := c.locality.Find("region")
	if clientRegion == "" {
		// If we do not have the source, there is no way to find the multiplier.
		log.VErrEventf(ctx, 2, "missing region tier in current node: locality=%s",
			c.locality.String())
		return tenantcostmodel.NetworkCost(0)
	}

	ruModel := c.ruModel.Load()
	if ruModel == nil {
		// This case is unlikely to happen since this method will only be
		// called through tenant processes, which has a KV interceptor.
		return tenantcostmodel.NetworkCost(0)
	}

	cost := tenantcostmodel.NetworkCost(0)
	if isWrite {
		for i := range targetRange.Replicas().Descriptors() {
			if replicaRegion, ok := c.getReplicaRegion(ctx, &targetRange.Replicas().Descriptors()[i]); ok {
				cost += ruModel.NetworkCost(tenantcostmodel.NetworkPath{
					ToRegion:   replicaRegion,
					FromRegion: clientRegion,
				})
			}
		}
	} else {
		if replicaRegion, ok := c.getReplicaRegion(ctx, targetReplica); ok {
			cost = ruModel.NetworkCost(tenantcostmodel.NetworkPath{
				ToRegion:   clientRegion,
				FromRegion: replicaRegion,
			})
		}
	}

	return cost
}

func (c *tenantSideCostController) getReplicaRegion(
	ctx context.Context, replica *roachpb.ReplicaDescriptor,
) (region string, ok bool) {
	nodeDesc, err := c.nodeDescs.GetNodeDescriptor(replica.NodeID)
	if err != nil {
		log.VErrEventf(ctx, 2, "node %d is not gossiped: %v", replica.NodeID, err)
		// If we don't know where a node is, we can't determine the network cost
		// for the operation.
		return "", false
	}

	region, ok = nodeDesc.Locality.Find("region")
	if !ok {
		log.VErrEventf(ctx, 2, "missing region locality for n %d", nodeDesc.NodeID)
		return "", false
	}

	return region, true
}

// UpdateEstimatedWriteReplicationBytes computes all the network paths taken by
// the leaseholder when it replicates writes to the other replicas for the given
// range, and then increments the corresponding metrics by the given writeBytes
// amount.
func (c *tenantSideCostController) UpdateEstimatedWriteReplicationBytes(
	ctx context.Context,
	targetRange *roachpb.RangeDescriptor,
	targetReplica *roachpb.ReplicaDescriptor,
	writeBytes int64,
) {
	fromNode, err := c.nodeDescs.GetNodeDescriptor(targetReplica.NodeID)
	if err != nil {
		// If we don't know where a node is, we can't determine the network
		// path for the operation.
		log.VErrEventf(ctx, 2, "node %d is not gossiped: %v", targetReplica.NodeID, err)
		return
	}
	for _, replica := range targetRange.Replicas().Descriptors() {
		if replica.ReplicaID == targetReplica.ReplicaID {
			continue
		}
		toNode, err := c.nodeDescs.GetNodeDescriptor(replica.NodeID)
		if err != nil {
			// If we don't know where a node is, we can't determine the network
			// path for the operation.
			log.VErrEventf(ctx, 2, "node %d is not gossiped: %v", replica.NodeID, err)
			continue
		}

		c.metrics.EstimatedReplicationBytesForPath(
			targetReplica.NodeID, fromNode.Locality,
			replica.NodeID, toNode.Locality,
		).Inc(writeBytes)
	}
}

// calculateBackgroundCPUSecs returns the number of CPU seconds estimated to
// have been consumed by background work triggered by this node during the given
// interval. This is proportional to the amount of estimated CPU consumed by
// this node during that interval, and is capped to a maximum fixed amount, as
// specified in the model.
func calculateBackgroundCPUSecs(
	cpuModel *tenantcostmodel.EstimatedCPUModel,
	globalEstimatedCPURate float64,
	deltaTime time.Duration,
	localEstimatedCPUSecs float64,
) float64 {
	// Determine how much eCPU was consumed by all SQL nodes during the time
	// interval. The global eCPU rate can be unavailable or stale, so use the
	// local rate if it's larger.
	elapsedSeconds := float64(deltaTime) / float64(time.Second)
	globalEstimatedCPUSecs :=
		math.Max(globalEstimatedCPURate*elapsedSeconds, localEstimatedCPUSecs)
	globalEstimatedCPURate = globalEstimatedCPUSecs / elapsedSeconds

	// Determine how much CPU was consumed by overhead during the last tick.
	// If there was less than "Amortization" eCPUs of usage, then only add
	// a portion of the overhead.
	backgroundCPUSecs := float64(cpuModel.BackgroundCPU.Amount) * elapsedSeconds
	if globalEstimatedCPURate < cpuModel.BackgroundCPU.Amortization {
		backgroundCPUSecs *= globalEstimatedCPURate / cpuModel.BackgroundCPU.Amortization
	}

	// This node is responsible for its percentage share of background usage.
	backgroundCPUSecs *= localEstimatedCPUSecs / globalEstimatedCPUSecs
	return backgroundCPUSecs
}
