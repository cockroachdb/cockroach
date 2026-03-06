// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

func registerContentionMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "contention/mixed-version",
		Owner: registry.OwnerObservability,
		// 4 nodes to maximize the chance that the gateway and leaseholder
		// are on different versions during the mixed-version phase.
		Cluster: r.MakeClusterSpec(4),
		// Disabled on IBM because s390x is only built on master and mixed-version
		// is impossible to test as of 05/2025.
		CompatibleClouds: registry.AllClouds.NoAWS().NoIBM(),
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Monitor:          true,
		Randomized:       true,
		Run:              runContentionMixedVersion,
	})
}

// runContentionMixedVersion verifies that serialization conflicts work
// correctly during mixed-version upgrades. This is a regression test for
// protobuf compatibility of the ConflictKey field added to
// TransactionRetryError and TransactionRetryWithProtoRefreshError. Adding an
// optional protobuf field is backward-compatible, but this test ensures that
// mixed-version clusters handle serialization conflicts without crashing and
// that the error is correctly propagated across nodes running different
// versions.
func runContentionMixedVersion(ctx context.Context, t test.Test, c cluster.Cluster) {
	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, c.All(),
		mixedversion.NeverUseFixtures,
		mixedversion.EnabledDeploymentModes(mixedversion.SystemOnlyDeployment),
		// The ConflictKey field was added in 26.1 and backported to 25.4.
		// Only run hooks once the cluster is at least at v25.4 so that
		// write buffering and other required settings are available.
		mixedversion.MinimumSupportedVersion("v25.4.0"),
	)

	mvt.OnStartup("set up contention test table", func(
		ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
	) error {
		// Disable write buffering to ensure lock acquisition produces
		// contention as expected.
		if err := h.Exec(rng, `SET CLUSTER SETTING kv.transaction.write_buffering.enabled = false`); err != nil {
			return err
		}
		if err := h.Exec(rng, `CREATE TABLE contention_test (k INT PRIMARY KEY, v STRING)`); err != nil {
			return err
		}
		return h.Exec(rng, `INSERT INTO contention_test VALUES (1, 'a'), (2, 'b')`)
	})

	mvt.InMixedVersion("serialization conflict across versions", func(
		ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
	) error {
		return causeSerializationConflict(ctx, l, rng, h)
	})

	mvt.AfterUpgradeFinalized("serialization conflict after finalization", func(
		ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
	) error {
		return causeSerializationConflict(ctx, l, rng, h)
	})

	mvt.Run()
}

// causeSerializationConflict creates serialization conflicts between
// transactions, iterating over all available nodes as both gateway and
// leaseholder to ensure cross-version error propagation is exercised.
//
// The conflict pattern works as follows:
//  1. txn1 reads key 1 at timestamp ts1
//  2. txn2 writes a write intent on key 1 at ts2 > ts1
//  3. txn2 reads key 2, bumping the timestamp cache on key 2 to ts2
//  4. txn1 writes key 2, but gets pushed past ts2 by the timestamp cache
//  5. txn1 tries to commit and must refresh its read of key 1 from ts1 to
//     ts_pushed, but txn2's intent at ts2 blocks the refresh -> 40001 error
func causeSerializationConflict(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper,
) error {
	nodes := h.AvailableNodes()
	if len(nodes) < 2 {
		l.Printf("fewer than 2 available nodes; skipping contention test")
		return nil
	}

	// For each available node, transfer the lease to that node and then
	// run the conflict from every other node as gateway. This ensures we
	// exercise both directions: old-leaseholder/new-gateway and
	// new-leaseholder/old-gateway.
	//
	// With replication factor 3 on a 4-node cluster, not every node holds
	// a replica, so lease transfers to non-replica nodes are skipped.
	for _, leaseNode := range nodes {
		l.Printf("transferring contention_test lease to n%d", leaseNode)
		db := h.Connect(leaseNode)
		if _, err := db.ExecContext(ctx,
			`ALTER TABLE contention_test EXPERIMENTAL_RELOCATE LEASE SELECT $1, 1`, leaseNode,
		); err != nil {
			if strings.Contains(err.Error(), "lease target replica not found") {
				l.Printf("n%d does not hold a replica; skipping", leaseNode)
				continue
			}
			return errors.Wrapf(err, "relocating lease to n%d", leaseNode)
		}

		for _, gatewayNode := range nodes {
			if gatewayNode == leaseNode {
				continue
			}
			if err := causeSerializationConflictOnNodes(ctx, l, rng, h, gatewayNode, leaseNode); err != nil {
				return err
			}
		}
	}
	return nil
}

// causeSerializationConflictOnNodes runs the serialization conflict
// pattern with txn1 on gatewayNode and txn2 on leaseNode. The lease
// for contention_test should already be on leaseNode so that txn1's
// KV requests are forwarded cross-node.
func causeSerializationConflictOnNodes(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	h *mixedversion.Helper,
	gatewayNode, leaseNode int,
) error {
	l.Printf("running serialization conflict test: gateway=n%d, leaseholder=n%d", gatewayNode, leaseNode)

	db1 := h.Connect(gatewayNode)
	db2 := h.Connect(leaseNode)

	// Step 1: txn1 reads key 1, establishing a read timestamp and a read
	// set that will need refreshing if txn1's timestamp is pushed.
	tx1, err := db1.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin txn1")
	}
	defer func() { _ = tx1.Rollback() }()

	if _, err := tx1.ExecContext(ctx, "SELECT * FROM contention_test WHERE k = 1"); err != nil {
		return errors.Wrap(err, "txn1 select")
	}

	// Step 2: txn2 writes to key 1, creating a write intent that conflicts
	// with txn1's read set.
	tx2, err := db2.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin txn2")
	}
	defer func() { _ = tx2.Rollback() }()

	val := fmt.Sprintf("conflict-%d", rng.Int())
	if _, err := tx2.ExecContext(ctx, "UPDATE contention_test SET v = $1 WHERE k = 1", val); err != nil {
		return errors.Wrap(err, "txn2 update k=1")
	}

	// Step 3: txn2 reads key 2. This bumps the timestamp cache on key 2 to
	// txn2's read timestamp, which will force txn1's subsequent write to
	// key 2 to be pushed forward.
	if _, err := tx2.ExecContext(ctx, "SELECT * FROM contention_test WHERE k = 2"); err != nil {
		return errors.Wrap(err, "txn2 select k=2")
	}

	// Step 4: txn1 writes key 2. The timestamp cache (bumped by txn2's read
	// in step 3) pushes txn1's write timestamp forward past txn2's timestamp.
	val = fmt.Sprintf("push-%d", rng.Int())
	_, txn1UpdateErr := tx1.ExecContext(ctx, "UPDATE contention_test SET v = $1 WHERE k = 2", val)

	// Step 5: txn1 tries to commit. Because its timestamp was pushed (step 4),
	// it must refresh its earlier read of key 1 (step 1). The refresh fails
	// because txn2 has a pending write intent on key 1 (step 2) at a timestamp
	// within the refresh window. This produces a RETRY_SERIALIZABLE (40001) error.
	var txn1CommitErr error
	if txn1UpdateErr == nil {
		txn1CommitErr = tx1.Commit()
	}

	// Commit txn2 regardless of txn1's outcome.
	if err := tx2.Commit(); err != nil {
		return errors.Wrap(err, "txn2 commit")
	}

	// Verify we got the expected serialization conflict. The Go pq driver
	// surfaces the RETRY_SERIALIZABLE reason in the error string rather than
	// the raw SQLSTATE 40001 code.
	var conflictErr error
	for _, e := range []error{txn1UpdateErr, txn1CommitErr} {
		if e != nil && strings.Contains(e.Error(), "RETRY_SERIALIZABLE") {
			conflictErr = e
			break
		}
	}

	if conflictErr == nil {
		return fmt.Errorf("expected txn1 (gateway=n%d, leaseholder=n%d) to hit a serialization conflict "+
			"(RETRY_SERIALIZABLE) but it did not (update_err=%v, commit_err=%v)",
			gatewayNode, leaseNode, txn1UpdateErr, txn1CommitErr)
	}
	l.Printf("txn1 (gateway=n%d, leaseholder=n%d) hit expected serialization conflict: %v",
		gatewayNode, leaseNode, conflictErr)

	return nil
}
