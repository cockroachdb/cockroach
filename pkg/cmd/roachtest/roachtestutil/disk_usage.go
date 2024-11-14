// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
)

// DiskUsageLogger regularly logs the disk spaced used by the nodes in the cluster.
type DiskUsageLogger struct {
	t      test.Test
	c      cluster.Cluster
	doneCh chan struct{}
}

// NewDiskUsageLogger populates a DiskUsageLogger.
func NewDiskUsageLogger(t test.Test, c cluster.Cluster) *DiskUsageLogger {
	return &DiskUsageLogger{
		t:      t,
		c:      c,
		doneCh: make(chan struct{}),
	}
}

// Done instructs the Runner to terminate.
func (dul *DiskUsageLogger) Done() {
	close(dul.doneCh)
}

// Runner runs in a loop until Done() is called and prints the cluster-wide per
// node disk usage in descending order.
func (dul *DiskUsageLogger) Runner(ctx context.Context) error {
	l, err := dul.t.L().ChildLogger("diskusage")
	if err != nil {
		return err
	}
	quietLogger, err := dul.t.L().ChildLogger("diskusage-exec", logger.QuietStdout, logger.QuietStderr)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dul.doneCh:
			return nil
		case <-ticker.C:
		}

		type usage struct {
			nodeNum int
			bytes   int
		}

		var bytesUsed []usage
		for i := 1; i <= dul.c.Spec().NodeCount; i++ {
			cur, err := GetDiskUsageInBytes(ctx, dul.c, quietLogger, i)
			if err != nil {
				// This can trigger spuriously as compactions remove files out from under `du`.
				l.Printf("%s", errors.Wrapf(err, "node #%d", i))
				cur = -1
			}
			bytesUsed = append(bytesUsed, usage{
				nodeNum: i,
				bytes:   cur,
			})
		}
		sort.Slice(bytesUsed, func(i, j int) bool { return bytesUsed[i].bytes > bytesUsed[j].bytes }) // descending

		var s []string
		for _, usage := range bytesUsed {
			s = append(s, fmt.Sprintf("n#%d: %s", usage.nodeNum, humanizeutil.IBytes(int64(usage.bytes))))
		}

		l.Printf("%s\n", strings.Join(s, ", "))
	}
}

// DiskUsageTracker can grab the disk usage of the provided cluster.
//
// TODO(msbutler): deprecate this, once restore roachtests also use prom setup.
type DiskUsageTracker struct {
	c cluster.Cluster
	l *logger.Logger
}

// GetDiskUsage sums the disk usage for the given nodes in megabytes.
func (du *DiskUsageTracker) GetDiskUsage(ctx context.Context, nodes option.NodeListOption) int {
	var usage int
	for _, n := range nodes {
		cur, err := GetDiskUsageInBytes(ctx, du.c, du.l, n)
		if err != nil {
			du.l.Printf("Unable to get disk usage for node %d", n)
			return 0
		}
		usage += cur
	}
	return usage / 1e6
}

func NewDiskUsageTracker(
	c cluster.Cluster, parentLogger *logger.Logger,
) (*DiskUsageTracker, error) {
	diskLogger, err := parentLogger.ChildLogger("disk-usage", logger.QuietStdout)
	if err != nil {
		return nil, err
	}
	return &DiskUsageTracker{c: c, l: diskLogger}, nil
}

// GetDiskUsageInBytes executes the command `du {store-dir}` on node `nodeIdx` and
// parses the result to bytes. nodeIdx starts at one.
func GetDiskUsageInBytes(
	ctx context.Context, c cluster.Cluster, logger *logger.Logger, nodeIdx int,
) (int, error) {
	var result install.RunResultDetails
	var err error
	// Retry a few times since `du` can warn if files get removed out from under
	// it (which happens during pebble compactions, for example).
	result, err = c.RunWithDetailsSingleNode(
		ctx,
		logger,
		option.WithNodes(c.Node(nodeIdx)).WithShouldRetryFn(install.AlwaysTrue),
		"du {store-dir} -sk 2>/dev/null | grep -oE '^[0-9]+'",
	)
	if err != nil {
		return 0, err
	}

	// We need this check because sometimes the first line of the roachprod output is a warning
	// about adding an ip to a list of known hosts.
	if strings.Contains(result.Stdout, "Warning") {
		result.Stdout = strings.Split(result.Stdout, "\n")[1]
	}

	size, err := strconv.Atoi(strings.TrimSpace(result.Stdout))
	if err != nil {
		return 0, err
	}

	return size * 1024, nil
}
