// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
)

const defaultGitloadRepoPath = "/tmp/gitload-repo"

// runGitloadVerify runs the full gitload oracle verification by invoking
// `cockroach workload check gitload` on the workload node. This uses the
// same source of truth (verify.go) as the workload check command, including
// git-dependent checks (commit graph, metadata, file_latest, replay) and
// SQL-only checks (repo_stats, referential integrity, secondary indexes).
//
// If a workload cluster is provided (via --workload-cluster), the command
// is executed on that cluster's first node. Otherwise, it runs locally on
// the machine executing roachtest, which requires a cockroach binary in
// $PATH and the git repo at the expected path.
func runGitloadVerify(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	// Get a pgurl that targets the gitload database.
	urls, err := c.ExternalPGUrl(ctx, o.L(), c.Node(1), roachprod.PGURLOptions{
		VirtualClusterName: roachtestflags.VirtualCluster,
		Database:           "gitload",
	})
	if err != nil {
		o.Fatal(err)
	}
	if len(urls) == 0 {
		o.Fatal("no pgurls returned")
	}

	if wc := o.WorkloadCluster(); wc != nil {
		cmd := roachtestutil.NewCommand("./%s workload check gitload", o.ClusterCockroach()).
			Flag("repo", defaultGitloadRepoPath).
			Arg(fmt.Sprintf("'%s'", urls[0]))

		o.Status("running gitload verification on workload cluster")
		result, err := wc.RunWithDetailsSingleNode(
			ctx, o.L(), option.WithNodes(wc.Node(1)), cmd.String(),
		)
		if err != nil {
			o.Fatalf("gitload verification failed: %v\nstdout: %s\nstderr: %s",
				err, result.Stdout, result.Stderr)
		}
		o.Status(fmt.Sprintf("gitload verification passed\n%s", result.Stdout))
	} else {
		// Downgrade sslmode for local execution: the node certificate may
		// not pass the local Go TLS verifier's strict X.509 checks.
		localURL := strings.Replace(urls[0], "sslmode=verify-full", "sslmode=require", 1)
		cmd := roachtestutil.NewCommand("./%s workload check gitload", o.ClusterCockroach()).
			Flag("repo", defaultGitloadRepoPath).
			Arg(fmt.Sprintf("'%s'", localURL))

		o.Status("running gitload verification locally")
		out, err := exec.CommandContext(ctx, "bash", "-c", cmd.String()).CombinedOutput()
		if err != nil {
			o.Fatalf("gitload verification failed: %v\noutput: %s", err, string(out))
		}
		o.Status(fmt.Sprintf("gitload verification passed\n%s", string(out)))
	}

	return nil
}

func registerGitloadVerify(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "gitload-verify",
		Owner:              registry.OwnerTestEng,
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runGitloadVerify,
	})
}
