// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/workload/cli"
)

type kvWorkload struct{}

var _ workloadType = kvWorkload{}

func (w kvWorkload) operations() []string {
	return []string{"write", "read", "follower-read"}
}

func (w kvWorkload) initWorkload(ctx context.Context, v variations) error {
	initCmd := fmt.Sprintf("./cockroach workload init kv --db target --splits %d {pgurl:1}", v.splits)
	return v.RunE(ctx, option.WithNodes(v.Node(1)), initCmd)
}

// Don't run a workload against the node we're going to shut down.
func (w kvWorkload) runWorkload(
	ctx context.Context, v variations, duration time.Duration, maxRate int,
) (*workloadData, error) {
	runCmd := fmt.Sprintf(
		`./cockroach workload run kv --db target --display-format=incremental-json --duration=%s --max-rate=%d --tolerate-errors `+
			`--max-block-bytes=%d --min-block-bytes=%d --read-percent=50 --follower-read-percent=50 --concurrency=500 {pgurl%s}`,
		duration,
		maxRate,
		v.blockSize,
		v.blockSize,
		v.stableNodes(),
	)
	allOutput, err := v.RunWithDetails(ctx, nil, option.WithNodes(v.workloadNodes()), runCmd)
	if err != nil {
		return nil, err
	}
	wd := workloadData{
		score: v.calculateScore,
		data:  make(map[string]map[time.Time]trackedStat),
	}
	for _, output := range allOutput {
		stdout := output.Stdout
		ticks := cli.ParseOutput(strings.NewReader(stdout))
		wd.addTicks(ticks)
	}
	return &wd, nil
}
