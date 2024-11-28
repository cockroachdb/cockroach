// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdctest

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// MakeRangeFeedValueReader starts rangefeed on the specified table and returns a function
// that returns the next *kvpb.RangeFeedValue from the table.
// This funciton is intended to be used in tests that wish to read low level roachpb.KeyValue(s).
// Instead of trying to generate KVs ourselves (subject to encoding restrictions, etc), it is
// simpler to just "INSERT ..." into the table, and then use this function to read next value.
func MakeRangeFeedValueReader(
	t testing.TB, execCfgI interface{}, desc catalog.TableDescriptor,
) (func(t testing.TB) *kvpb.RangeFeedValue, func()) {
	reader, cleanup := MakeRangeFeedValueReaderExtended(t, execCfgI, desc)
	wrapMultiPurposeReader := func(t testing.TB) *kvpb.RangeFeedValue {
		val, delRange := reader(t)
		if delRange != nil {
			t.Fatal("RangeFeedDeleteRange encountered but is not supported by the caller. " +
				"Use MakeRangeFeedValueReaderExtended instead.")
		}
		return val
	}
	return wrapMultiPurposeReader, cleanup
}

// MakeRangeFeedValueReaderExtended is like MakeRangeFeedValueReader,
// but it can return a RangeFeedDeleteRange too.
func MakeRangeFeedValueReaderExtended(
	t testing.TB, execCfgI interface{}, desc catalog.TableDescriptor,
) (func(t testing.TB) (*kvpb.RangeFeedValue, *kvpb.RangeFeedDeleteRange), func()) {
	t.Helper()
	execCfg := execCfgI.(sql.ExecutorConfig)

	// Rangefeeds might still work even when this setting is false because span
	// configs may enable them, but relying on span configs can be prone to
	// issues as seen in #109507. Therefore, we assert that the cluster setting
	// is set.
	require.True(t, kvserver.RangefeedEnabled.Get(&execCfg.Settings.SV))

	rows := make(chan *kvpb.RangeFeedValue)
	ctx, cleanup := context.WithCancel(context.Background())
	deleteRangeC := make(chan *kvpb.RangeFeedDeleteRange)

	_, err := execCfg.RangeFeedFactory.RangeFeed(ctx, "feed-"+desc.GetName(),
		[]roachpb.Span{desc.PrimaryIndexSpan(execCfg.Codec)},
		execCfg.Clock.Now(),
		func(ctx context.Context, value *kvpb.RangeFeedValue) {
			select {
			case <-ctx.Done():
			case rows <- value:
			}
		},
		rangefeed.WithDiff(true),
		rangefeed.WithOnDeleteRange(func(ctx context.Context, e *kvpb.RangeFeedDeleteRange) {
			select {
			case deleteRangeC <- e:
			case <-ctx.Done():
			}
		}),
	)
	require.NoError(t, err)

	var timeout = 10 * time.Second
	if util.RaceEnabled {
		timeout = 3 * timeout
	}

	// Helper to read next rangefeed value.
	dups := make(map[string]struct{})
	return func(t testing.TB) (*kvpb.RangeFeedValue, *kvpb.RangeFeedDeleteRange) {
		t.Helper()
		for {
			select {
			case r := <-rows:
				rowKey := r.Key.String() + r.Value.String()
				if _, isDup := dups[rowKey]; isDup {
					log.Infof(context.Background(), "Skip duplicate %s", roachpb.PrettyPrintKey(nil, r.Key))
					continue
				}
				log.Infof(context.Background(), "Read row %s", roachpb.PrettyPrintKey(nil, r.Key))
				dups[rowKey] = struct{}{}
				return r, nil
			case d := <-deleteRangeC:
				return nil, d
			case <-time.After(timeout):
				t.Fatal("timeout reading row")
				return nil, nil
			}
		}
	}, cleanup
}

// GetHydratedTableDescriptor returns a table descriptor for the specified
// table.  The descriptor is "hydrated" if it has user defined data types.
func GetHydratedTableDescriptor(
	t testing.TB, execCfgI interface{}, parts ...tree.Name,
) (td catalog.TableDescriptor) {
	t.Helper()
	dbName, scName, tableName := func() (tree.Name, tree.Name, tree.Name) {
		switch len(parts) {
		case 1:
			return "defaultdb", "public", parts[0]
		case 2:
			return parts[0], "public", parts[1]
		case 3:
			return parts[0], parts[1], parts[2]
		default:
			t.Fatal("invalid length")
			return "", "", ""
		}
	}()

	execCfg := execCfgI.(sql.ExecutorConfig)
	require.NoError(t, sql.DescsTxn(context.Background(), &execCfg,
		func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
			_, td, err = descs.PrefixAndTable(ctx, col.ByName(txn.KV()).Get(), tree.NewTableNameWithSchema(dbName, scName, tableName))
			return err
		}))
	require.NotNil(t, td)
	return td
}
