// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"bufio"
	"bytes"
	"context"
	"regexp"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTenantsStorageMetrics ensures that tenant storage metrics are properly
// set upon split. There's two interesting cases:
//
//  1) The common case where the RHS and LHS of the split are co-located
//  2) The rare case where the RHS of the split has already been removed from
//     the store by the time the LHS applies the split.
//
// This test at time of writing only deals with ensuring that 1) is covered.
// TODO(ajwerner): add a test for 2).
func TestTenantsStorageMetricsOnSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})

	ctx := context.Background()
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	defer store.Stopper().Stop(ctx)

	tenantID := roachpb.MakeTenantID(10)
	codec := keys.MakeSQLCodec(tenantID)

	tenantPrefix := codec.TenantPrefix()
	_, _, err = s.SplitRange(tenantPrefix)
	require.NoError(t, err)

	splitKey := codec.TablePrefix(42)
	_, _, err = s.SplitRange(splitKey)
	require.NoError(t, err)

	require.NoError(t, db.Put(ctx, codec.TablePrefix(41), "bazbax"))
	require.NoError(t, db.Put(ctx, splitKey, "foobar"))

	// We want it to be the case that the MVCC stats for the individual ranges
	// of our tenant line up with the metrics for the tenant.
	check := func() error {
		var aggregateStats enginepb.MVCCStats
		var seen int
		store.VisitReplicas(func(replica *kvserver.Replica) (wantMore bool) {
			ri := replica.State()
			if ri.TenantID != tenantID.ToUint64() {
				return true
			}
			seen++
			aggregateStats.Add(*ri.Stats)
			return true
		})
		ex := metric.MakePrometheusExporter()
		ex.ScrapeRegistry(store.Registry(), true /* includeChildMetrics */)
		var in bytes.Buffer
		if err := ex.PrintAsText(&in); err != nil {
			t.Fatalf("failed to print prometheus data: %v", err)
		}
		if seen != 2 {
			return errors.Errorf("expected to see two replicas for our tenant, saw %d", seen)
		}
		sc := bufio.NewScanner(&in)
		re := regexp.MustCompile(`^(\w+)\{.*,tenant_id="` + tenantID.String() + `"\} (\d+)`)
		metricsToVal := map[string]int64{
			"gcbytesage":  aggregateStats.GCBytesAge,
			"intentage":   aggregateStats.IntentAge,
			"livebytes":   aggregateStats.LiveBytes,
			"livecount":   aggregateStats.LiveCount,
			"keybytes":    aggregateStats.KeyBytes,
			"keycount":    aggregateStats.KeyCount,
			"valbytes":    aggregateStats.ValBytes,
			"valcount":    aggregateStats.ValCount,
			"intentbytes": aggregateStats.IntentBytes,
			"intentcount": aggregateStats.IntentCount,
			"sysbytes":    aggregateStats.SysBytes,
			"syscount":    aggregateStats.SysCount,
		}
		for sc.Scan() {
			matches := re.FindAllStringSubmatch(sc.Text(), 1)
			if matches == nil {
				continue
			}
			metric, valStr := matches[0][1], matches[0][2]
			exp, ok := metricsToVal[matches[0][1]]
			if !ok {
				continue
			}
			val, err := strconv.ParseInt(valStr, 10, 64)
			require.NoError(t, err)
			if exp != val {
				return errors.Errorf("value for %s %d != %d", metric, val, exp)
			}
			delete(metricsToVal, metric)
		}
		if len(metricsToVal) != 0 {
			return errors.Errorf("did not see all metrics, still expect %v", metricsToVal)
		}
		return nil
	}
	testutils.SucceedsSoon(t, check)

}
