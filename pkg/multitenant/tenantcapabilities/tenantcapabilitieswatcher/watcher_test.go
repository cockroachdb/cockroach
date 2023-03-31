// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitieswatcher_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiestestutils"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitieswatcher"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDataDriven runs datadriven tests against the
// tenentcapabilitieswatcher.Watcher struct. The syntax is as follows:
//
// "start": starts the Watcher.
//
// "upsert" and "delete": updates the underlying global tenant capability state.
// Example:
//
// upsert ten=10 can_admin_split=true
// ----
// ok
//
// delete ten=15
// ----
// ok
//
// "updates": lists out updates observed by the watcher after the underlying
// tenant capability state has been updated.
//
// "get-capabilities": prints out the capabilities for a supplied tenant.
//
// "flush-state": flushes the in-memory state maintained by the Watcher.
//
// "inject-error": injects an error into the Watcher's underlying rangefeed. For
// testing purposes, instead of retrying and establishing a new rangefeed, the
// Watcher instead blocks until the test indicates otherwise. See
// restart-after-injected-error.
//
// "restart-after-injected-error": Unblocks the watcher from restarting the
// underlying rangefeed after an injected error. Should only be used after an
// error was indeed injected to have much meaning.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer tc.Stopper().Stop(ctx)

		ts := tc.Server(0)
		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
		tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)

		const dummyTableName = "dummy_system_tenants"
		tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.tenants INCLUDING ALL)", dummyTableName))

		var dummyTableID uint32
		tdb.QueryRow(t, fmt.Sprintf(
			`SELECT table_id FROM crdb_internal.tables WHERE name = '%s'`, dummyTableName),
		).Scan(&dummyTableID)

		mu := struct {
			syncutil.Mutex
			lastFrontierTS     hlc.Timestamp // serializes updates and update-state
			receivedUpdates    []tenantcapabilities.Update
			receivedUpdateType rangefeedcache.UpdateType
			rangeFeedRunning   bool
		}{}

		errorInjectionCh := make(chan error)
		restartAfterErrCh := make(chan struct{})
		defer func() {
			close(restartAfterErrCh)
		}()

		watcher := tenantcapabilitieswatcher.New(
			ts.Clock(),
			ts.RangeFeedFactory().(*rangefeed.Factory),
			dummyTableID,
			ts.Stopper(),
			1<<20, /* 1 MB */
			&tenantcapabilities.TestingKnobs{
				WatcherTestingKnobs: &tenantcapabilitieswatcher.TestingKnobs{
					WatcherRangeFeedKnobs: &rangefeedcache.TestingKnobs{
						PostRangeFeedStart: func() {
							mu.Lock()
							defer mu.Unlock()

							mu.rangeFeedRunning = true
						},
						OnTimestampAdvance: func(ts hlc.Timestamp) {
							mu.Lock()
							defer mu.Unlock()
							mu.lastFrontierTS = ts
						},
						ErrorInjectionCh: errorInjectionCh,
						PreExit: func() {
							mu.Lock()
							mu.rangeFeedRunning = false
							mu.Unlock()
							// Block until the test directives indicate otherwise.
							<-restartAfterErrCh
						},
					},
					WatcherUpdatesInterceptor: func(UpdateType rangefeedcache.UpdateType, updates []tenantcapabilities.Update) {
						mu.Lock()
						defer mu.Unlock()
						mu.receivedUpdates = append(mu.receivedUpdates, updates...)
						mu.receivedUpdateType = UpdateType
					},
				},
			})

		var lastUpdateTS hlc.Timestamp
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "start":
				err := watcher.Start(ctx)
				require.NoError(t, err)
				// Wait for the underlying rangefeed to have started before returning.
				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()
					if !mu.rangeFeedRunning {
						return errors.New("expected underlying rangefeed to have started")
					}
					return nil
				})

			case "updates":
				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()

					// No updates to be observed if the underlying rangefeed isn't
					// running.
					if !mu.rangeFeedRunning {
						return nil
					}

					if lastUpdateTS.Less(mu.lastFrontierTS) {
						return nil
					}

					return errors.Newf("frontier timestamp (%s) lagging last update (%s)",
						mu.lastFrontierTS.String(), lastUpdateTS.String())
				})

				mu.Lock()
				receivedUpdates := mu.receivedUpdates
				mu.receivedUpdates = mu.receivedUpdates[:0] // clear out buffer
				updateType := mu.receivedUpdateType
				mu.Unlock()

				// De-duplicate updates. We want a stable sort here because the
				// underlying slice is timestamp ordered, which is something we rely on
				// (and thus test for).
				sort.SliceStable(receivedUpdates, func(i, j int) bool {
					return receivedUpdates[i].TenantID.ToUint64() < receivedUpdates[j].TenantID.ToUint64()
				})
				var output strings.Builder
				for i := range receivedUpdates {
					if i == 0 {
						output.WriteString(fmt.Sprintf("%s\n", updateType))
					}
					if i+1 != len(receivedUpdates) && receivedUpdates[i+1].TenantID.Equal(receivedUpdates[i].TenantID) {
						continue // de-duplicate
					}
					if receivedUpdates[i].Deleted {
						output.WriteString(fmt.Sprintf("delete: ten=%v\n", receivedUpdates[i].TenantID))
					} else {
						output.WriteString(fmt.Sprintf("update: ten=%v cap=%v\n", receivedUpdates[i].TenantID, tenantcapabilitiestestutils.AlteredCapabilitiesString(receivedUpdates[i].TenantCapabilities)))
					}
				}
				return output.String()

			case "upsert":
				tenID, caps, err := tenantcapabilitiestestutils.ParseTenantCapabilityUpsert(t, d)
				require.NoError(t, err)
				info := mtinfopb.ProtoInfo{
					Capabilities: *caps,
				}
				buf, err := protoutil.Marshal(&info)
				require.NoError(t, err)
				tdb.Exec(
					t,
					fmt.Sprintf("UPSERT INTO %s (id, active, info) VALUES ($1, $2, $3)", dummyTableName),
					tenID.ToUint64(),
					true, /* active */
					buf,
				)
				lastUpdateTS = ts.Clock().Now()
			case "delete":
				delete := tenantcapabilitiestestutils.ParseTenantCapabilityDelete(t, d)
				tdb.Exec(
					t,
					fmt.Sprintf("DELETE FROM %s WHERE id = $1", dummyTableName),
					delete.TenantID.ToUint64(),
				)
				lastUpdateTS = ts.Clock().Now()

			case "get-capabilities":
				tID := tenantcapabilitiestestutils.GetTenantID(t, d)
				cp, found := watcher.GetCapabilities(tID)
				if !found {
					return "not-found"
				}
				return fmt.Sprintf("%v", tenantcapabilitiestestutils.AlteredCapabilitiesString(cp))

			case "flush-state":
				var output strings.Builder
				entries := watcher.TestingFlushCapabilitiesState()
				for _, entry := range entries {
					output.WriteString(fmt.Sprintf("ten=%v cap=%v\n", entry.TenantID, tenantcapabilitiestestutils.AlteredCapabilitiesString(entry.TenantCapabilities)))
				}
				return output.String()
			case "inject-error":
				err := errors.New("big-yikes")
				errorInjectionCh <- err
				// Ensure the thing actually stopped.
				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()
					if mu.rangeFeedRunning {
						return errors.New("expected underlying rangefeed to have stopped")
					}
					return nil
				})
				return err.Error()

			case "restart-after-injected-error":
				restartAfterErrCh <- struct{}{}
				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()
					if !mu.rangeFeedRunning {
						return errors.New("expected undrelying rangefeed to have restarted")
					}
					return nil
				})

			default:
				return fmt.Sprintf("unknown command %s", d.Cmd)
			}
			return "ok"
		})
	})
}
