// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvsubscriber_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDataDriven runs datadriven tests against the KVSubscriber interface.
// The syntax is as follows:
//
//      kvaccessor-update
//      delete [c,e)
//      upsert [c,d):C
//      upsert [d,e):D
//      ----
//      ok
//
//      kvaccessor-get
//      span [a,b)
//      span [b,c)
//      ----
//      [a,b):A
//      [b,d):B
//
//      kvsubscriber-start
//      ----
//      ok
//
//      kvsubscriber-updates
//      ----
//      [a,b)
//      [b,d)
//      [e,f)
//
//      kvsubscriber-reader key=b
//      ----
//      [b,d):B
//
//      kvsubscriber-reader compute-split=[a,c)
//      ----
//      b
//
//      kvsubscriber-reader needs-split=[b,h)
//      ----
//      true
//
// - kvaccessor-{update,get} tie into GetSpanConfigEntriesFor and
//   UpdateSpanConfigEntries respectively on the KVAccessor interface, and are a
//   convenient shorthand to populate the system table that the KVSubscriber
//   subscribes to. The input is processed in a single batch.
// - kvsubscriber-start starts the subscriber process
// - kvsubscriber-updates lists the span updates the KVSubscriber receives,
//   in the listed order. Updates in a batch are de-duped.
// - kvsubscriber-reader {key,compute-split,needs-split} relate to
//   GetSpanConfigForKey, ComputeSplitKey and NeedsSplit respectively on the
//   StoreReader subset of the KVSubscriber interface.
//
// Text of the form [a,b) and [a,b):C correspond to spans and span config
// entries; see spanconfigtestutils.Parse{Span,Config,SpanConfigEntry} for more
// details.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				EnableSpanConfigs: true,
			},
		})
		defer tc.Stopper().Stop(ctx)

		ts := tc.Server(0)
		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
		tdb.Exec(t, `SET CLUSTER SETTING spanconfig.experimental_kvaccessor.enabled = true`)
		tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)

		const dummyTableName = "dummy_span_configurations"
		tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummyTableName))

		var dummyTableID uint32
		tdb.QueryRow(t, fmt.Sprintf(
			`SELECT table_id from crdb_internal.tables WHERE name = '%s'`, dummyTableName),
		).Scan(&dummyTableID)

		kvAccessor := spanconfigkvaccessor.New(
			tc.Server(0).DB(),
			tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
			tc.Server(0).ClusterSettings(),
			fmt.Sprintf("defaultdb.public.%s", dummyTableName),
		)

		mu := struct {
			syncutil.Mutex
			lastFrontierTS  hlc.Timestamp
			receivedUpdates []roachpb.Span
		}{}
		kvSubscriber := spanconfigkvsubscriber.New(
			tc.Stopper(),
			ts.DB(),
			ts.Clock(),
			ts.RangeFeedFactory().(*rangefeed.Factory),
			dummyTableID,
			10<<20, /* 10 MB */
			spanconfigtestutils.ParseConfig(t, "MISSING"),
			&spanconfig.TestingKnobs{
				KVSubscriberOnFrontierAdvanceInterceptor: func(ts hlc.Timestamp) {
					mu.Lock()
					mu.lastFrontierTS = ts
					defer mu.Unlock()
				},
			},
		)

		kvSubscriber.SubscribeToKVUpdates(ctx, func(span roachpb.Span) {
			mu.Lock()
			defer mu.Unlock()
			mu.receivedUpdates = append(mu.receivedUpdates, span)
		})

		var lastUpdateTS hlc.Timestamp
		subscriberStarted := false

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "kvaccessor-get":
				spans := spanconfigtestutils.ParseKVAccessorGetArguments(t, d.Input)
				entries, err := kvAccessor.GetSpanConfigEntriesFor(ctx, spans)
				require.NoError(t, err)

				var output strings.Builder
				for _, entry := range entries {
					output.WriteString(fmt.Sprintf("%s\n", spanconfigtestutils.PrintSpanConfigEntry(entry)))
				}
				return output.String()

			case "kvaccessor-update":
				toDelete, toUpsert := spanconfigtestutils.ParseKVAccessorUpdateArguments(t, d.Input)
				require.NoError(t, kvAccessor.UpdateSpanConfigEntries(ctx, toDelete, toUpsert))
				lastUpdateTS = ts.Clock().Now()
				return "ok"

			case "kvsubscriber-start":
				require.NoError(t, kvSubscriber.Start(ctx))
				subscriberStarted = true
				return "ok"

			case "kvsubscriber-updates":
				require.True(t, subscriberStarted, "expected subscriber to have been started")

				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()

					if mu.lastFrontierTS.Less(lastUpdateTS) {
						return errors.Newf("frontier timestamp (%s) lagging last update (%s)",
							mu.lastFrontierTS.String(), lastUpdateTS.String())
					}
					return nil
				}) // TODO(irfansharif): We could use a tighter bound here, but it's unreliable under stress.

				mu.Lock()
				defer mu.Unlock()

				var output strings.Builder
				for i, update := range mu.receivedUpdates {
					seen := false
					for j := 0; j < i; j++ {
						if mu.receivedUpdates[i].Equal(mu.receivedUpdates[j]) {
							seen = true
						}
					}
					if seen {
						continue // de-dup updates
					}

					var spanStr string
					if update.Equal(keys.EverythingSpan) {
						spanStr = update.String()
					} else {
						spanStr = spanconfigtestutils.PrintSpan(update)
					}
					output.WriteString(fmt.Sprintf("%s\n", spanStr))
				}

				mu.receivedUpdates = mu.receivedUpdates[:0] // clear out the updates
				return output.String()

			case "kvsubscriber-reader":
				require.True(t, subscriberStarted, "expected subscriber to have been started")

				if len(d.CmdArgs) != 1 {
					d.Fatalf(t, "unexpected number of args (%d), expected 1", len(d.CmdArgs))
				}
				cmdArg := d.CmdArgs[0]

				switch cmdArg.Key {
				case "key":
					var keyStr string
					d.ScanArgs(t, cmdArg.Key, &keyStr)
					config, err := kvSubscriber.GetSpanConfigForKey(ctx, roachpb.RKey(keyStr))
					require.NoError(t, err)
					return fmt.Sprintf("conf=%s", spanconfigtestutils.PrintSpanConfig(config))

				case "compute-split":
					var spanStr string
					d.ScanArgs(t, cmdArg.Key, &spanStr)
					span := spanconfigtestutils.ParseSpan(t, spanStr)
					start, end := roachpb.RKey(span.Key), roachpb.RKey(span.EndKey)
					splitKey := kvSubscriber.ComputeSplitKey(ctx, start, end)
					return string(splitKey)

				case "needs-split":
					var spanStr string
					d.ScanArgs(t, cmdArg.Key, &spanStr)
					span := spanconfigtestutils.ParseSpan(t, spanStr)
					start, end := roachpb.RKey(span.Key), roachpb.RKey(span.EndKey)
					result := kvSubscriber.NeedsSplit(ctx, start, end)
					return fmt.Sprintf("%t", result)

				default:
					t.Fatalf("unknown argument: %s", cmdArg.Key)
				}

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}
