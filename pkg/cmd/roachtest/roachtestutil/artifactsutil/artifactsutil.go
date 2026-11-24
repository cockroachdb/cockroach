// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package artifactsutil

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
)

// CollectRemoteArtifacts fetches srcDir from each cluster node into the test's
// local artifacts directory. Each node's data lands in
// <artifactsDir>/<nodeIdx>.<dstName>/. For example, with srcDir="perf" and
// dstName="perf", node 3's artifacts end up in <artifactsDir>/3.perf/.
//
// Nodes are fetched concurrently. Nodes where srcDir does not exist are
// silently skipped.
func CollectRemoteArtifacts(
	ctx context.Context, t test.Test, c cluster.Cluster, srcDir, dstName string,
) {
	fetchNode := func(ctx context.Context, node int) error {
		testCmd := fmt.Sprintf(
			`'if [ -d "%s" ]; then echo true; else echo false; fi'`, srcDir)
		result, err := c.RunWithDetailsSingleNode(
			ctx, t.L(), option.WithNodes(c.Node(node)), "bash", "-c", testCmd)
		if err != nil {
			return errors.Wrapf(err, "checking for %s on n%d", srcDir, node)
		}
		if strings.TrimSpace(result.Stdout) != "true" {
			return nil
		}
		dst := fmt.Sprintf("%s/%d.%s", t.ArtifactsDir(), node, dstName)
		return c.Get(ctx, t.L(), srcDir, dst, c.Node(node))
	}
	g := ctxgroup.WithContext(ctx)
	for _, node := range c.All() {
		g.GoCtx(func(ctx context.Context) error {
			return fetchNode(ctx, node)
		})
	}
	if err := g.Wait(); err != nil {
		t.L().PrintfCtx(ctx, "failed to collect %s artifacts: %v", srcDir, err)
	}
}

// CollectPerfArtifacts is a convenience wrapper around CollectRemoteArtifacts
// for the perf artifacts directory.
//
// The test runner calls this automatically on success. Tests should call it
// explicitly before t.Fatal when valid perf data exists but the test is about
// to fail (e.g. threshold violations in benchmarks).
func CollectPerfArtifacts(ctx context.Context, t test.Test, c cluster.Cluster) {
	CollectRemoteArtifacts(ctx, t, c, t.PerfArtifactsDir(), "perf")
}
