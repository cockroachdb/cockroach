package modular

import (
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func newModTest() *Test {
	cfg := logger.Config{
		Stderr: io.Discard,
		Stdout: io.Discard,
	}
	l, err := cfg.NewLogger("" /*path*/)
	if err != nil {
		panic(err)
	}

	test := NewTest(context.Background(), l, nil, nil)
	return test
}

// TestBasicDAG tests the DAG generation using an echo test.
func TestBasicDAG(t *testing.T) {
	mod := newModTest()

	baselineStage := mod.NewStage("baseline")
	chaosStage := mod.NewStage("chaos")

	// Install prometheus and grafana, then after that is done, start a background loop to scrape metrics.
	mod.Setup("installing prom" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
		return nil
	})
	mod.Setup("starting histogram scrape loop" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
		return nil
	})
	mod.Setup("importing tpcc workload" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
		return nil
	})

	for _, stage := range []*Stage{baselineStage, chaosStage} {
		// Run TPCC workload
		stage.Execute("running TPCC workload for 1 hour" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
			return nil
		}).Then("dropping TPCC tables" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
			return nil
		})

		stage.Execute("increasing replication factor to 5" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
			return nil
		}).Then("waiting for replication" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
			return nil
		}).Then("sleeping for 10 minutes" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
			return nil
		}).Then("decreasing replication factor to 3" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
			return nil
		})

		stage.Execute("copy bank table" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
			return nil
		})

		// TODO: the framework itself should add this automatically when chaos is enabled.
		if stage == chaosStage {
			stage.Execute("inject network partition" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
				return nil
			}).Then("recover from network partition" /* step name */, func(ctx context.Context, l *logger.Logger, h *Helper) error {
				return nil
			})
		}
	}

	// Add after-test step, which is run after prometheus scraping is turned off.
	mod.AfterTest("TPCC consistency checks", func(ctx context.Context, l *logger.Logger, h *Helper) error {
		return nil
	})
	plan := mod.Plan()
	t.Log(plan.String())
}

/*
Test -> []Stage
Stage -> []Execute groups of steps -> [Sequential, Concurrent]
Execute -> []steps

stage 1
[]sequence can run concurrently


stage 2
*/
