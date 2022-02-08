// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
)

// TargetPeriodSetting is exported for testing purposes.
var TargetPeriodSetting = settings.RegisterDurationSetting(
	settings.TenantReadOnly,
	"tenant_cost_control_period",
	"target duration between token bucket requests from tenants (requires restart)",
	10*time.Second,
	checkDurationInRange(5*time.Second, 120*time.Second),
)

// CPUUsageAllowance is exported for testing purposes.
var CPUUsageAllowance = settings.RegisterDurationSetting(
	settings.TenantReadOnly,
	"tenant_cpu_usage_allowance",
	"this much CPU usage per second is considered background usage and "+
		"doesn't contribute to consumption; for example, if it is set to 10ms, "+
		"that corresponds to 1% of a CPU",
	10*time.Millisecond,
	checkDurationInRange(0, 1000*time.Millisecond),
)

// checkDurationInRange returns a function used to validate duration cluster
// settings. Because these values are currently settable by the tenant, we need
// to restrict the allowed values to avoid possible sabotage of the cost control
// mechanisms.
func checkDurationInRange(min, max time.Duration) func(v time.Duration) error {
	return func(v time.Duration) error {
		if v < min || v > max {
			return errors.Errorf("value %s out of range (%s, %s)", v, min, max)
		}
		return nil
	}
}

// mainLoopUpdateInterval is the period at which we collect CPU usage and
// evaluate whether we need to send a new token request.
const mainLoopUpdateInterval = 1 * time.Second

// movingAvgFactor is the weight applied to a new "sample" of RU usage (with one
// sample per mainLoopUpdateInterval).
//
// If we want a factor of 0.5 per second, this should be:
//   0.5^(1 second / mainLoopUpdateInterval)
const movingAvgFactor = 0.5

// We request more tokens when the available RUs go below a threshold. The
// threshold is a fraction of the last granted RUs.
const notifyFraction = 0.1

// When we trickle RUs over a period of time, we request more tokens a bit
// before that period runs out. This "anticipation" should be more than what we
// expect the RTT of a token bucket request to be in practice.
const anticipation = time.Second

// If we have less than this many RUs to report, extend the reporting period to
// reduce load on the host cluster.
const consumptionReportingThreshold = 100

// The extended reporting period is this factor times the normal period.
const extendedReportingPeriodFactor = 4

// We try to maintain this many RUs in our local bucket, regardless of estimated
// usage. This is intended to support usage spikes without blocking.
const bufferRUs = 5000

func newTenantSideCostController(
	st *cluster.Settings,
	tenantID roachpb.TenantID,
	provider kvtenant.TokenBucketProvider,
	timeSource timeutil.TimeSource,
	testInstr TestInstrumentation,
) (multitenant.TenantSideCostController, error) {
	if tenantID == roachpb.SystemTenantID {
		return nil, errors.AssertionFailedf("cost controller can't be used for system tenant")
	}
	c := &tenantSideCostController{
		timeSource:      timeSource,
		testInstr:       testInstr,
		settings:        st,
		tenantID:        tenantID,
		provider:        provider,
		responseChan:    make(chan *roachpb.TokenBucketResponse, 1),
		lowRUNotifyChan: make(chan struct{}, 1),
	}
	c.limiter.Init(timeSource, testInstr, c.lowRUNotifyChan)

	c.costCfg = tenantcostmodel.ConfigFromSettings(&st.SV)
	return c, nil
}

// NewTenantSideCostController creates an object which implements the
// server.TenantSideCostController interface.
func NewTenantSideCostController(
	st *cluster.Settings, tenantID roachpb.TenantID, provider kvtenant.TokenBucketProvider,
) (multitenant.TenantSideCostController, error) {
	return newTenantSideCostController(
		st, tenantID, provider,
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
	timeSource timeutil.TimeSource,
	testInstr TestInstrumentation,
) (multitenant.TenantSideCostController, error) {
	return newTenantSideCostController(st, tenantID, provider, timeSource, testInstr)
}

func init() {
	server.NewTenantSideCostController = NewTenantSideCostController
}

type tenantSideCostController struct {
	timeSource           timeutil.TimeSource
	testInstr            TestInstrumentation
	settings             *cluster.Settings
	costCfg              tenantcostmodel.Config
	tenantID             roachpb.TenantID
	provider             kvtenant.TokenBucketProvider
	limiter              limiter
	stopper              *stop.Stopper
	instanceID           base.SQLInstanceID
	sessionID            sqlliveness.SessionID
	externalUsageFn      multitenant.ExternalUsageFn
	nextLiveInstanceIDFn multitenant.NextLiveInstanceIDFn

	mu struct {
		syncutil.Mutex

		consumption roachpb.TenantConsumption
	}

	// lowRUNotifyChan is used when the number of available RUs is running low and
	// we need to send an early token bucket request.
	lowRUNotifyChan chan struct{}

	// responseChan is used to receive results from token bucket requests, which
	// are run in a separate goroutine. A nil response indicates an error.
	responseChan chan *roachpb.TokenBucketResponse

	// run contains the state that is updated by the main loop.
	run struct {
		now time.Time
		// externalUsage stores the last value returned by externalUsageFn.
		externalUsage multitenant.ExternalUsage
		// consumption stores the last value of mu.consumption.
		consumption roachpb.TenantConsumption
		// requestSeqNum is an increasing sequence number that is included in token
		// bucket requests.
		requestSeqNum int64

		// targetPeriod stores the value of the TargetPeriodSetting setting at the
		// last update.
		targetPeriod time.Duration

		// initialRequestCompleted is set to true when the first token bucket
		// request completes successfully.
		initialRequestCompleted bool

		// requestInProgress is true if we are in the process of sending a request;
		// it gets set to false when we process the response (in the main loop),
		// even in error cases.
		requestInProgress bool

		// requestNeedsRetry is set if the last token bucket request encountered an
		// error. This triggers a retry attempt on the next tick.
		//
		// Note: requestNeedsRetry and requestInProgress are never true at the same
		// time.
		requestNeedsRetry bool

		lastRequestTime         time.Time
		lastReportedConsumption roachpb.TenantConsumption

		lastDeadline time.Time
		lastRate     float64

		// When we obtain tokens that are throttled over a period of time, we set up
		// a low RU notification only when we get close to that period of time
		// elapsing.
		setupNotificationTimer     timeutil.TimerI
		setupNotificationCh        <-chan time.Time
		setupNotificationThreshold tenantcostmodel.RU

		// fallbackRate is the refill rate we fall back to if the token bucket
		// requests don't complete or take a long time.
		fallbackRate float64
		// fallbackRateStart is the time when we can switch to the fallback rate;
		// set only when we get a low RU notification.
		fallbackRateStart time.Time

		// avgRUPerSec is an exponentially-weighted moving average of the RU
		// consumption per second; used to estimate the RU requirements for the next
		// request.
		avgRUPerSec float64
		// lastSecRU is the consumption.RU value when avgRUPerSec was last updated.
		avgRUPerSecLastRU float64
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

func (c *tenantSideCostController) initRunState(ctx context.Context) {
	c.run.targetPeriod = TargetPeriodSetting.Get(&c.settings.SV)

	now := c.timeSource.Now()
	c.run.now = now
	c.run.externalUsage = c.externalUsageFn(ctx)
	c.run.lastRequestTime = now
	c.run.avgRUPerSec = initialRUs / c.run.targetPeriod.Seconds()
	c.run.requestSeqNum = 1
}

// updateRunState is called whenever the main loop awakens and accounts for the
// CPU usage in the interim.
func (c *tenantSideCostController) updateRunState(ctx context.Context) {
	c.run.targetPeriod = TargetPeriodSetting.Get(&c.settings.SV)

	newTime := c.timeSource.Now()
	newExternalUsage := c.externalUsageFn(ctx)

	// Update CPU consumption.

	deltaCPU := newExternalUsage.CPUSecs - c.run.externalUsage.CPUSecs

	// Subtract any allowance that we consider free background usage.
	if deltaTime := newTime.Sub(c.run.now); deltaTime > 0 {
		deltaCPU -= CPUUsageAllowance.Get(&c.settings.SV).Seconds() * deltaTime.Seconds()
	}
	if deltaCPU < 0 {
		deltaCPU = 0
	}
	ru := deltaCPU * float64(c.costCfg.PodCPUSecond)

	var deltaPGWireEgressBytes uint64
	if newExternalUsage.PGWireEgressBytes > c.run.externalUsage.PGWireEgressBytes {
		deltaPGWireEgressBytes = newExternalUsage.PGWireEgressBytes - c.run.externalUsage.PGWireEgressBytes
		ru += float64(deltaPGWireEgressBytes) * float64(c.costCfg.PGWireEgressByte)
	}

	c.mu.Lock()
	c.mu.consumption.SQLPodsCPUSeconds += deltaCPU
	c.mu.consumption.PGWireEgressBytes += deltaPGWireEgressBytes
	c.mu.consumption.RU += ru
	newConsumption := c.mu.consumption
	c.mu.Unlock()

	c.run.now = newTime
	c.run.externalUsage = newExternalUsage
	c.run.consumption = newConsumption

	c.limiter.RemoveTokens(newTime, tenantcostmodel.RU(ru))
}

// updateAvgRUPerSec is called exactly once per mainLoopUpdateInterval.
func (c *tenantSideCostController) updateAvgRUPerSec() {
	delta := c.run.consumption.RU - c.run.avgRUPerSecLastRU
	c.run.avgRUPerSec = movingAvgFactor*c.run.avgRUPerSec + (1-movingAvgFactor)*delta
	c.run.avgRUPerSecLastRU = c.run.consumption.RU
}

// shouldReportConsumption decides if it's time to send a token bucket request
// to report consumption.
func (c *tenantSideCostController) shouldReportConsumption() bool {
	if c.run.requestInProgress {
		return false
	}

	timeSinceLastRequest := c.run.now.Sub(c.run.lastRequestTime)
	if timeSinceLastRequest >= c.run.targetPeriod {
		consumptionToReport := c.run.consumption.RU - c.run.lastReportedConsumption.RU
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
	deltaConsumption := c.run.consumption
	deltaConsumption.Sub(&c.run.lastReportedConsumption)

	var requested float64

	if !c.run.initialRequestCompleted {
		requested = initialRUs
	} else {
		// Request what we expect to need over the next target period plus the
		// buffer amount.
		requested = c.run.avgRUPerSec*c.run.targetPeriod.Seconds() + bufferRUs

		// Adjust by the currently available amount. If we are in debt, we request
		// more to cover the debt.
		requested -= float64(c.limiter.AvailableTokens(c.run.now))
		if requested < 0 {
			// We don't need more RUs right now, but we still want to report
			// consumption.
			requested = 0
		}
	}

	req := roachpb.TokenBucketRequest{
		TenantID:                    c.tenantID.ToUint64(),
		InstanceID:                  uint32(c.instanceID),
		InstanceLease:               c.sessionID.UnsafeBytes(),
		NextLiveInstanceID:          uint32(c.nextLiveInstanceIDFn(ctx)),
		SeqNum:                      c.run.requestSeqNum,
		ConsumptionSinceLastRequest: deltaConsumption,
		RequestedRU:                 requested,
		TargetRequestPeriod:         c.run.targetPeriod,
	}
	c.run.requestSeqNum++

	c.run.lastRequestTime = c.run.now
	// TODO(radu): in case of an error, we undercount some consumption.
	c.run.lastReportedConsumption = c.run.consumption
	c.run.requestInProgress = true

	ctx, _ = c.stopper.WithCancelOnQuiesce(ctx)
	err := c.stopper.RunAsyncTask(ctx, "token-bucket-request", func(ctx context.Context) {
		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Infof(ctx, "issuing TokenBucket: %s\n", req.String())
		}
		resp, err := c.provider.TokenBucket(ctx, &req)
		if err != nil {
			// Don't log any errors caused by the stopper canceling the context.
			if !errors.Is(err, context.Canceled) {
				log.Warningf(ctx, "TokenBucket RPC error: %v", err)
			}
			resp = nil
		} else if (resp.Error != errorspb.EncodedError{}) {
			// This is a "logic" error which indicates a configuration problem on the
			// host side. We will keep retrying periodically.
			err := errors.DecodeError(ctx, resp.Error)
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
	ctx context.Context, resp *roachpb.TokenBucketResponse,
) {
	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Infof(
			ctx, "TokenBucket response: %g RUs over %s (fallback rate %g)",
			resp.GrantedRU, resp.TrickleDuration, resp.FallbackRate,
		)
	}
	c.run.fallbackRate = resp.FallbackRate

	if !c.run.initialRequestCompleted {
		c.run.initialRequestCompleted = true
		// This is the first successful request. Take back the initial RUs that we
		// used to pre-fill the bucket.
		c.limiter.RemoveTokens(c.run.now, initialRUs)
	}

	granted := resp.GrantedRU
	if granted == 0 {
		// We must have not requested any more RUs. The token bucket state doesn't
		// need updating.
		//
		// It is possible that we got a low RU notification while the request was in
		// flight. If that is the case, fallbackRateStart will be set and we send
		// another request.
		if !c.run.fallbackRateStart.IsZero() {
			c.sendTokenBucketRequest(ctx)
		}
		return
	}
	// It doesn't matter if we received a notification; we are going to
	// reconfigure the bucket and set up a new notification as needed.
	c.run.fallbackRateStart = time.Time{}

	if !c.run.lastDeadline.IsZero() {
		// If last request came with a trickle duration, we may have RUs that were
		// not made available to the bucket yet; throw them together with the newly
		// granted RUs.
		if since := c.run.lastDeadline.Sub(c.run.now); since > 0 {
			granted += c.run.lastRate * since.Seconds()
		}
	}

	if c.run.setupNotificationTimer != nil {
		c.run.setupNotificationTimer.Stop()
		c.run.setupNotificationTimer = nil
		c.run.setupNotificationCh = nil
	}

	notifyThreshold := tenantcostmodel.RU(granted * notifyFraction)
	if notifyThreshold < bufferRUs {
		notifyThreshold = bufferRUs
	}
	var cfg tokenBucketReconfigureArgs
	if resp.TrickleDuration == 0 {
		// We received a batch of tokens to use as needed. Set up the token bucket
		// to notify us when the tokens are running low.
		cfg.NewTokens = tenantcostmodel.RU(granted)
		// TODO(radu): if we don't get more tokens in time, fall back to a "fallback"
		// rate.
		cfg.NewRate = 0
		cfg.NotifyThreshold = notifyThreshold

		c.run.lastDeadline = time.Time{}
	} else {
		// We received a batch of tokens that can only be used over the
		// TrickleDuration. Set up the token bucket to notify us a bit before this
		// period elapses (unless we accumulate enough unused tokens, in which case
		// we get notified when the tokens are running low).
		deadline := c.run.now.Add(resp.TrickleDuration)

		cfg.NewRate = tenantcostmodel.RU(granted / resp.TrickleDuration.Seconds())

		timerDuration := resp.TrickleDuration - anticipation
		if timerDuration <= 0 {
			timerDuration = (resp.TrickleDuration + 1) / 2
		}

		c.run.setupNotificationTimer = c.timeSource.NewTimer()
		c.run.setupNotificationTimer.Reset(timerDuration)
		c.run.setupNotificationCh = c.run.setupNotificationTimer.Ch()
		c.run.setupNotificationThreshold = notifyThreshold

		c.run.lastDeadline = deadline
	}
	c.run.lastRate = float64(cfg.NewRate)
	c.limiter.Reconfigure(c.run.now, cfg)
}

func (c *tenantSideCostController) mainLoop(ctx context.Context) {
	interval := mainLoopUpdateInterval
	// Make sure the interval is never larger than the target request period. This
	// is useful for tests which set a very small period.
	if targetPeriod := TargetPeriodSetting.Get(&c.settings.SV); targetPeriod < interval {
		interval = targetPeriod
	}
	ticker := c.timeSource.NewTicker(interval)
	defer ticker.Stop()
	tickerCh := ticker.Ch()

	c.initRunState(ctx)
	c.sendTokenBucketRequest(ctx)

	// The main loop should never block. The remote requests run in separate
	// goroutines.
	for {
		select {
		case <-tickerCh:
			c.updateRunState(ctx)
			c.updateAvgRUPerSec()

			// Switch to the fallback rate, if necessary.
			if !c.run.fallbackRateStart.IsZero() && !c.run.now.Before(c.run.fallbackRateStart) {
				log.Infof(ctx, "switching to fallback rate %.10g", c.run.fallbackRate)
				c.limiter.Reconfigure(c.run.now, tokenBucketReconfigureArgs{
					NewRate: tenantcostmodel.RU(c.run.fallbackRate),
				})
				c.run.fallbackRateStart = time.Time{}
			}
			if c.run.requestNeedsRetry || c.shouldReportConsumption() {
				c.run.requestNeedsRetry = false
				c.sendTokenBucketRequest(ctx)
			}
			if c.testInstr != nil {
				c.testInstr.Event(c.run.now, TickProcessed)
			}

		case resp := <-c.responseChan:
			c.run.requestInProgress = false
			if resp != nil {
				c.updateRunState(ctx)
				c.handleTokenBucketResponse(ctx, resp)
				if c.testInstr != nil {
					c.testInstr.Event(c.run.now, TokenBucketResponseProcessed)
				}
			} else {
				// A nil response indicates a failure (which would have been logged).
				c.run.requestNeedsRetry = true
			}

		case <-c.run.setupNotificationCh:
			c.run.setupNotificationTimer = nil
			c.run.setupNotificationCh = nil

			c.updateRunState(ctx)
			c.limiter.SetupNotification(c.run.now, c.run.setupNotificationThreshold)

		case <-c.lowRUNotifyChan:
			c.updateRunState(ctx)
			c.run.fallbackRateStart = c.run.now.Add(anticipation)
			// If we have a request in flight, the token bucket will get reconfigured.
			if !c.run.requestInProgress {
				c.sendTokenBucketRequest(ctx)
			}
			if c.testInstr != nil {
				c.testInstr.Event(c.run.now, LowRUNotification)
			}

		case <-c.stopper.ShouldQuiesce():
			c.limiter.Close()
			// TODO(radu): send one last request to update consumption.
			return
		}
	}
}

// OnRequestWait is part of the multitenant.TenantSideKVInterceptor
// interface.
func (c *tenantSideCostController) OnRequestWait(
	ctx context.Context, info tenantcostmodel.RequestInfo,
) error {
	if multitenant.HasTenantCostControlExemption(ctx) {
		return nil
	}
	// Note that the tenantSideController might not be started yet; that is ok
	// because we initialize the limiter with some initial RUs and a reasonable
	// initial rate.
	return c.limiter.Wait(ctx, c.costCfg.RequestCost(info))
}

// OnResponse is part of the multitenant.TenantSideBatchInterceptor interface.
//
// TODO(radu): we don't get a callback in error cases (ideally we should return
// the RequestCost to the bucket).
func (c *tenantSideCostController) OnResponse(
	ctx context.Context, req tenantcostmodel.RequestInfo, resp tenantcostmodel.ResponseInfo,
) {
	if multitenant.HasTenantCostControlExemption(ctx) {
		return
	}
	if resp.ReadBytes() > 0 {
		c.limiter.RemoveTokens(c.timeSource.Now(), c.costCfg.ResponseCost(resp))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if isWrite, writeBytes := req.IsWrite(); isWrite {
		c.mu.consumption.WriteRequests++
		c.mu.consumption.WriteBytes += uint64(writeBytes)
		c.mu.consumption.RU += float64(c.costCfg.KVWriteCost(writeBytes))
	} else {
		c.mu.consumption.ReadRequests++
		readBytes := resp.ReadBytes()
		c.mu.consumption.ReadBytes += uint64(readBytes)
		c.mu.consumption.RU += float64(c.costCfg.KVReadCost(readBytes))
	}
}
