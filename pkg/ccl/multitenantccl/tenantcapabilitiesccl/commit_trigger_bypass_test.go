// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilitiesccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTenantCannotForgeCommitTriggers is an end-to-end regression test for
// VULM-477: a secondary tenant must not be able to attach an
// InternalCommitTrigger to an ordinary EndTxn. Commit triggers drive privileged
// range operations (splits, merges, replica changes, sticky-bit updates) and
// are only ever issued internally by KV. Before the fix, the capability layer
// inspected only an EndTxn's method and Prepare flag — never its trigger — so a
// tenant could smuggle one through and mutate its own range descriptors without
// holding the relevant capability.
func TestTenantCannotForgeCommitTriggers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer tc.Stopper().Stop(ctx)

	tenantID := roachpb.MustMakeTenantID(10)
	tenant, err := tc.Server(0).TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID: tenantID,
	})
	require.NoError(t, err)
	tenantDB := tenant.DB()
	sysSQL := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	codec := keys.MakeSQLCodec(tenantID)

	// Revoke the capability that gates range changes such as setting a
	// sticky bit, and confirm the supported path (AdminSplit) is rejected.
	// Poll, since the revocation propagates to the authorizer
	// asynchronously.
	sysSQL.Exec(t, "ALTER TENANT [10] GRANT CAPABILITY can_admin_split=false")
	splitExpiration := tc.Server(0).Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	testutils.SucceedsSoon(t, func() error {
		err := tenantDB.AdminSplit(ctx, codec.TablePrefix(100), splitExpiration)
		if err == nil {
			return errors.New("AdminSplit unexpectedly succeeded; capability not yet revoked")
		}
		require.Regexp(t, `does not have capability "can_admin_split"`, err)
		return nil
	})

	// The tenant's keyspace begins at a range boundary, so the range covering the
	// start of the keyspace is contained entirely within it. That is the range
	// the forged trigger targets.
	rangeDesc, err := tc.LookupRange(codec.TenantPrefix())
	require.NoError(t, err)
	startKey := rangeDesc.StartKey
	descKey := keys.RangeDescriptorKey(startKey)

	store := tc.GetFirstStoreFromServer(t, 0)
	stickyBitBefore := store.LookupReplica(startKey).Desc().StickyBit

	// Forge an EndTxn carrying a StickyBitTrigger, mirroring
	// splitTxnStickyUpdateAttempt in pkg/kv/kvserver/replica_command.go but
	// driven entirely by the tenant. The descriptor write addresses into
	// the tenant's keyspace and is allowed; the committing EndTxn that
	// carries the trigger must be rejected by the capability authorizer.
	forgedStickyBit := tc.Server(0).Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	require.False(t, forgedStickyBit.Equal(stickyBitBefore))
	err = tenantDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		existing, err := txn.Get(ctx, descKey)
		if err != nil {
			return err
		}
		oldDesc := &roachpb.RangeDescriptor{}
		if err := existing.Value.GetProto(oldDesc); err != nil {
			return err
		}
		newDesc := *oldDesc
		newDesc.StickyBit = forgedStickyBit
		newBytes, err := protoutil.Marshal(&newDesc)
		if err != nil {
			return err
		}

		b := txn.NewBatch()
		b.CPut(descKey, newBytes, existing.Value.TagAndDataBytes())
		if err := txn.Run(ctx, b); err != nil {
			return err
		}

		commit := txn.NewBatch()
		commit.AddRawRequest(&kvpb.EndTxnRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				StickyBitTrigger: &roachpb.StickyBitTrigger{
					StickyBit: forgedStickyBit,
				},
			},
		})
		return txn.Run(ctx, commit)
	})
	require.Error(t, err, "tenant was allowed to attach a commit trigger")
	require.Regexp(t, `internal commit triggers may only be issued by the system tenant`, err)

	// The rejected trigger must not have changed the range.
	require.True(t, store.LookupReplica(startKey).Desc().StickyBit.Equal(stickyBitBefore),
		"forged trigger mutated the range descriptor despite being rejected")

	// Granting the capability lets the tenant split, which internally
	// issues an EndTxn with a SplitTrigger under the tenant's context.  The
	// fix must not reject that.
	sysSQL.Exec(t, "ALTER TENANT [10] GRANT CAPABILITY can_admin_split=true")
	testutils.SucceedsSoon(t, func() error {
		return tenantDB.AdminSplit(ctx, codec.TablePrefix(200), splitExpiration)
	})
}
