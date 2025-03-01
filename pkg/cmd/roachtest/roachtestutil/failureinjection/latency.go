package failureinjection

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"time"
)

type ArtificialLatencyInjector struct {
	c      cluster.Cluster
	l      *logger.Logger
	failer failures.FailureMode
	args   failures.NetworkLatencyArgs
}

func MakeArtificialLatencyInjector(c cluster.Cluster, l *logger.Logger) (*ArtificialLatencyInjector, error) {
	latencyFailer, err := failures.MakeNetworkLatencyFailure(c.MakeNodes(), l, c.IsSecure())
	if err != nil {
		return nil, err
	}
	return &ArtificialLatencyInjector{
		c:      c,
		l:      l,
		failer: latencyFailer,
	}, nil
}

type OneWayLatencies struct {
	RegionA string
	RegionB string
	Latency time.Duration
}

// LatencyMap is a map of one way latencies between regions.
type LatencyMap map[string]map[string]time.Duration

func CreateLatencyMap(oneWayLatencies []OneWayLatencies) LatencyMap {
	latencyMap := make(map[string]map[string]time.Duration)
	for _, latencies := range oneWayLatencies {
		if _, ok := latencyMap[latencies.RegionA]; !ok {
			latencyMap[latencies.RegionA] = make(map[string]time.Duration)
		}
		if _, ok := latencyMap[latencies.RegionB]; !ok {
			latencyMap[latencies.RegionB] = make(map[string]time.Duration)
		}
		latencyMap[latencies.RegionA][latencies.RegionB] = latencies.Latency
		latencyMap[latencies.RegionB][latencies.RegionA] = latencies.Latency
	}
	return latencyMap
}

var MultiregionLatencyMap = func() LatencyMap {
	regionLatencies := []OneWayLatencies{
		{
			RegionA: "us-east",
			RegionB: "us-west",
			Latency: 33 * time.Millisecond,
		},
		{
			RegionA: "us-east",
			RegionB: "europe-west",
			Latency: 30 * time.Millisecond,
		},
		{
			RegionA: "us-west",
			RegionB: "europe-west",
			Latency: 70 * time.Millisecond,
		},
	}
	return CreateLatencyMap(regionLatencies)
}()

func (a *ArtificialLatencyInjector) CreateDefaultMultiRegionCluster(ctx context.Context) error {
	regionToNodeMap := make(map[string][]install.Node)
	for i, node := range a.c.All() {
		switch i % 3 {
		case 0:
			regionToNodeMap["us-east"] = append(regionToNodeMap["us-east"], install.Node(node))
		case 1:
			regionToNodeMap["us-west"] = append(regionToNodeMap["us-west"], install.Node(node))
		case 2:
			regionToNodeMap["europe-west"] = append(regionToNodeMap["europe-west"], install.Node(node))
		}
	}
	return a.CreateRegions(ctx, regionToNodeMap, MultiregionLatencyMap)
}

func (a *ArtificialLatencyInjector) CreateRegions(ctx context.Context, regionToNodeMap map[string][]install.Node, latencyMap LatencyMap) error {
	artificialLatencies := make([]failures.ArtificialLatency, 0)
	for regionA, srcNodes := range regionToNodeMap {
		for regionB, destNodes := range regionToNodeMap {
			if regionA == regionB {
				continue
			}
			delay := latencyMap[regionA][regionB]
			artificialLatencies = append(artificialLatencies, failures.ArtificialLatency{
				Source:      srcNodes,
				Destination: destNodes,
				Delay:       delay,
			})
		}
	}

	a.args = failures.NetworkLatencyArgs{
		ArtificialLatencies: artificialLatencies,
	}

	if err := a.failer.Setup(ctx, a.l, a.args); err != nil {
		return err
	}
	return a.failer.Inject(ctx, a.l, a.args)
}

func (a *ArtificialLatencyInjector) Restore(ctx context.Context) error {
	return a.failer.Restore(ctx, a.l, a.args)
}
