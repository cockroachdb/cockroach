package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type nodeHealthChecker struct {
	node   *Node
	latest struct {
		syncutil.Mutex
		collectionTime time.Time
		nodeStatus     *statuspb.NodeStatus
	}
}

type healthChecker struct {
	s       *Server
	stopper *stop.Stopper

	nodeHealth *nodeHealthChecker

	latest struct {
		syncutil.Mutex
		report *statuspb.HealthCheckResult
	}
}

func newNodeHealthChecker(n *Node) *nodeHealthChecker {
	return &nodeHealthChecker{node: n}
}

func newHealthChecker(s *Server, nh *nodeHealthChecker, stopper *stop.Stopper) *healthChecker {
	return &healthChecker{
		s:          s,
		stopper:    stopper,
		nodeHealth: nh,
	}
}

func (h *nodeHealthChecker) getLatestNodeStatus(
	ctx context.Context, forceNow bool,
) *statuspb.NodeStatus {
	h.latest.Lock()
	defer h.latest.Unlock()

	// If we have a node status collected already that's also recent enough, we can reuse it.
	if !forceNow &&
		timeutil.Now().Sub(h.latest.collectionTime) < healthCheckInterval {
		return h.latest.nodeStatus
	}

	nodeStatus := h.node.recorder.GenerateNodeStatus(ctx)
	if nodeStatus == nil {
		return nil
	}
	h.latest.nodeStatus = nodeStatus
	return nodeStatus
}

func (h *healthChecker) doHealthCheck(ctx context.Context) error {
	nodeStatus := h.nodeHealth.getLatestNodeStatus(ctx, false /* forceNow */)

	// Collect the node metrics and possible alerts.
	result := h.s.node.recorder.CheckMetricsHealth(ctx, *nodeStatus)

	result.CollectionTime = timeutil.Now().UnixNano()

	// If there are alerts, report them to gossip.
	if len(result.Alerts) != 0 {
		h.reportAlertsToGossip(ctx, &result)
	}

	// In any case, store the last health report.
	h.latest.Lock()
	defer h.latest.Unlock()
	h.latest.report = &result
	return nil
}

func (h *healthChecker) reportAlertsToGossip(
	ctx context.Context, result *statuspb.HealthCheckResult,
) {
	n := h.s.node
	var numNodes int
	if err := n.storeCfg.Gossip.IterateInfos(gossip.KeyNodeIDPrefix, func(k string, info gossip.Info) error {
		numNodes++
		return nil
	}); err != nil {
		log.Warningf(ctx, "%v", err)
	} else {
		if numNodes > 1 {
			// Avoid this warning on single-node clusters, which require special UX.
			log.Warningf(ctx, "health alerts detected: %+v", result.Alerts)
		}
	}
	if err := n.storeCfg.Gossip.AddInfoProto(
		// Use an alertTTL of twice the health check period. This makes sure that
		// alerts don't disappear and reappear spuriously while at the same
		// time ensuring that an alert doesn't linger for too long after having
		// resolved.
		gossip.MakeNodeHealthAlertKey(n.Descriptor.NodeID), result, 2*healthCheckInterval,
	); err != nil {
		log.Warningf(ctx, "unable to gossip health alerts: %+v", result.Alerts)
	}

	// TODO(tschottdorf): add a metric that we increment every time there are
	// alerts. This can help understand how long the cluster has been in that
	// state (since it'll be incremented every ~10s).
}

// FIXME: make this configurable
const healthCheckInterval = 5 * time.Second

func (h *healthChecker) startHealthChecks(ctx context.Context) error {
	return h.stopper.RunAsyncTask(ctx, "bg-health-check", func(ctx context.Context) {
		ticker := time.NewTicker(healthCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := h.doHealthCheck(ctx); err != nil {
					log.Ops.Warningf(ctx, "health check failed: %v", err)
				}

			case <-h.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

func (h *healthChecker) GetResult() *statuspb.HealthCheckResult {
	h.latest.Lock()
	defer h.latest.Unlock()
	res := h.latest.report
	if res == nil {
		res = &statuspb.HealthCheckResult{}
	}
	return res
}

func (h *healthChecker) handleDebug(w http.ResponseWriter, req *http.Request) {
	h.latest.Lock()
	defer h.latest.Unlock()
	json, err := json.Marshal(h.latest.report)
	if err != nil {
		log.Ops.Errorf(context.Background(), "cannot marshal health report as JSON: %v", err)
		http.Error(w, "internal error", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(json)
}
