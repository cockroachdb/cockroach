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
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
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
//      update
//      delete [c,e)
//      upsert [c,d):C
//      upsert [d,e):D
//      upsert {entire-keyspace}:X
//      delete {source=1,target=20}
//      ----
//
//      get
//      span [a,b)
//      span [b,c)
//      ----
//      [a,b):A
//      [b,d):B
//
//      start
//      ----
//
//      updates
//      ----
//      [a,b)
//      [b,d)
//      [e,f)
//
//      store-reader key=b
//      ----
//      [b,d):B
//
//      store-reader compute-split=[a,c)
//      ----
//      b
//
//      store-reader needs-split=[b,h)
//      ----
//      true
//
//      inject-buffer-overflow
//      ----
//      ok
//
// - update and get tie into GetSpanConfigRecords and UpdateSpanConfigRecords
//   respectively on the KVAccessor interface, and are a convenient shorthand to
//   populate the system table that the KVSubscriber subscribes to. The input is
//   processed in a single batch.
// - start starts the subscription process. It can also be used to verify
//   behavior when re-establishing subscriptions after hard errors.
// - updates lists the span updates the KVSubscriber receives, in the listed
//   order. Updates in a batch are de-duped.
// - store-reader {key,compute-split,needs-split} relate to GetSpanConfigForKey,
//   ComputeSplitKey and NeedsSplit respectively on the StoreReader subset of the
//   KVSubscriber interface.
// - inject-buffer-overflow can be used to inject rangefeed buffer overflow
//   errors within the kvsubscriber. It pokes into the internals of the
//   kvsubscriber and is useful to test teardown and recovery behavior.
//
// Text of the form [a,b) and [a,b):C correspond to spans and span config
// records; see spanconfigtestutils.Parse{Span,Config,SpanConfigRecord} for more
// details.
// TODO(arul): Add ability to express tenant spans to this datadriven test.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer cancel()
		defer tc.Stopper().Stop(ctx)

		ts := tc.Server(0)
		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
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
			lastFrontierTS    hlc.Timestamp // serializes updates and update
			subscriberRunning bool          // serializes updates, subscribe, and inject-buffer-overflow
			receivedUpdates   roachpb.Spans
		}{}
		injectedErrCh := make(chan error)

		kvSubscriber := spanconfigkvsubscriber.New(
			ts.Clock(),
			ts.RangeFeedFactory().(*rangefeed.Factory),
			dummyTableID,
			10<<20, /* 10 MB */
			spanconfigtestutils.ParseConfig(t, "FALLBACK"),
			&spanconfig.TestingKnobs{
				KVSubscriberRangeFeedKnobs: &rangefeedcache.TestingKnobs{
					OnTimestampAdvance: func(ts hlc.Timestamp) {
						mu.Lock()
						defer mu.Unlock()
						mu.lastFrontierTS = ts
					},
					PostRangeFeedStart: func() {
						mu.Lock()
						defer mu.Unlock()
						mu.subscriberRunning = true
					},
					PreExit: func() {
						mu.Lock()
						defer mu.Unlock()
						mu.subscriberRunning = false
					},
					ErrorInjectionCh: injectedErrCh,
				},
			},
		)

		kvSubscriber.Subscribe(func(ctx context.Context, span roachpb.Span) {
			mu.Lock()
			defer mu.Unlock()
			mu.receivedUpdates = append(mu.receivedUpdates, span)
		})

		var lastUpdateTS hlc.Timestamp
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "get":
				targets := spanconfigtestutils.ParseKVAccessorGetArguments(t, d.Input)
				records, err := kvAccessor.GetSpanConfigRecords(ctx, targets)
				require.NoError(t, err)

				var output strings.Builder
				for _, record := range records {
					output.WriteString(fmt.Sprintf("%s\n", spanconfigtestutils.PrintSpanConfigRecord(t, record)))
				}
				return output.String()

			case "update":
				toDelete, toUpsert := spanconfigtestutils.ParseKVAccessorUpdateArguments(t, d.Input)
				require.NoError(t, kvAccessor.UpdateSpanConfigRecords(ctx, toDelete, toUpsert))
				lastUpdateTS = ts.Clock().Now()

			case "start":
				mu.Lock()
				require.False(t, mu.subscriberRunning, "subscriber already running")
				mu.Unlock()

				go func() {
					_ = kvSubscriber.TestingRunInner(ctx)
				}()
				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()
					if !mu.subscriberRunning {
						return errors.New("expected subscriber to have started")
					}
					return nil
				})

			case "updates":
				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()

					if !mu.subscriberRunning {
						// The subscriber isn't running, we're not expecting any
						// frontier bumps.
						return nil
					}

					// The subscriber is running -- we should be observing
					// frontier bumps. In order to serialize after the last
					// kvaccessor-update, lets wait until the frontier timestamp
					// is past it.
					if lastUpdateTS.LessEq(mu.lastFrontierTS) {
						return nil
					}

					return errors.Newf("frontier timestamp (%s) lagging last update (%s)",
						mu.lastFrontierTS.String(), lastUpdateTS.String())
				}) // TODO(irfansharif): We could use a tighter bound here, but it's unreliable under stress.

				mu.Lock()
				receivedUpdates := mu.receivedUpdates
				mu.receivedUpdates = mu.receivedUpdates[:0] // clear out buffer
				mu.Unlock()

				var output strings.Builder
				sort.Sort(receivedUpdates)
				for i, update := range receivedUpdates {
					if i != 0 && receivedUpdates[i].Equal(receivedUpdates[i-1]) {
						continue // de-dup updates
					}
					output.WriteString(fmt.Sprintf("%s\n", spanconfigtestutils.PrintSpan(update)))
				}

				return output.String()

			case "inject-buffer-overflow":
				injectedErrCh <- rangefeedbuffer.ErrBufferLimitExceeded
				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()
					if mu.subscriberRunning {
						return errors.New("expected subscriber to have stopped")
					}
					return nil
				})

			case "store-reader":
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
					d.Fatalf(t, "unknown argument: %s", cmdArg.Key)
				}

			default:
				d.Fatalf(t, "unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}
