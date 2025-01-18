// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admit_long

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

type noTickManualTime struct {
	*timeutil.ManualTime
}

var _ timeutil.TimeSource = noTickManualTime{}

func (noTickManualTime) NewTicker(duration time.Duration) timeutil.TickerI {
	return noTickTicker{}
}

type noTickTicker struct{}

func (noTickTicker) Reset(duration time.Duration) {}
func (noTickTicker) Stop()                        {}
func (noTickTicker) Ch() <-chan time.Time         { return nil }

type testUsageReporter struct {
	t     *timeutil.ManualTime
	g     *workGranterImpl
	kind  WorkCategoryAndStore
	id    workID
	usage resourceUsageByWork
}

var _ internalUsageReporter = &testUsageReporter{}

func (r *testUsageReporter) WorkStart()                        {}
func (r *testUsageReporter) MeasureCPU(g int)                  {}
func (r *testUsageReporter) CumulativeWriteBytes(w WriteBytes) {}
func (r *testUsageReporter) WorkDone() {
	if r.usage.startTimeUnixNanos != 0 {
		r.usage.endTimeUnixNanos = r.t.Now().UnixNano()
	}
	r.usage.done = true
	r.g.workDone(r.kind, r.id, true)
}
func (r *testUsageReporter) GetIDForDebugging() uint64 { return 0 }
func (r *testUsageReporter) getMetrics() resourceUsageByWork {
	return r.usage
}
func (r *testUsageReporter) workRejected() {
	r.usage.done = true
	r.g.workDone(r.kind, r.id, false)
}

type testRequesterState struct {
	parent                   *testRequesterStates
	wcAndStore               WorkCategoryAndStore
	allowedWithoutPermission int
	score                    WorkScore
	acceptGrant              bool
	reporters                map[workID]*testUsageReporter
	b                        *strings.Builder
}

var _ WorkRequester = &testRequesterState{}

func (r *testRequesterState) GetAllowedWithoutPermissionForStore() int {
	fmt.Fprintf(r.b, "%+v.GetAllowedWithoutPermissionForStore(): %d\n",
		r.wcAndStore, r.allowedWithoutPermission)
	return r.allowedWithoutPermission
}

func (r *testRequesterState) GetScore() (bool, WorkScore) {
	fmt.Fprintf(r.b, "%+v.GetScore(): %+v\n", r.wcAndStore, r.score)
	return r.score != (WorkScore{}), r.score
}

func (r *testRequesterState) Grant(reporter WorkResourceUsageReporter) bool {
	tur := reporter.(*testUsageReporter)
	fmt.Fprintf(r.b, "%+v.Grant(%d): ", r.wcAndStore, tur.id)
	if !r.acceptGrant {
		fmt.Fprintf(r.b, "rejected\n")
		return false
	}
	fmt.Fprintf(r.b, "accepted\n")
	r.reporters[tur.id] = tur
	return true
}

type testRequesterStates struct {
	t          *timeutil.ManualTime
	requesters map[WorkCategoryAndStore]*testRequesterState
}

func (c *testRequesterStates) newUsageReporter(
	g *workGranterImpl, kind WorkCategoryAndStore, id workID, numGoroutines int,
) internalUsageReporter {
	reporter := &testUsageReporter{
		t:    c.t,
		g:    g,
		kind: kind,
		id:   id,
	}
	return reporter
}

type testResourceStatsProvider struct {
	stats StatsForResources
}

func (p *testResourceStatsProvider) GetStats(
	scratch map[roachpb.StoreID]ResourceStats,
) StatsForResources {
	if scratch == nil {
		scratch = map[roachpb.StoreID]ResourceStats{}
	}
	clear(scratch)
	for storeID, stats := range p.stats.Disk {
		scratch[storeID] = stats
	}
	return StatsForResources{
		CPU:  p.stats.CPU,
		Disk: scratch,
	}
}

type deltaStatsPrinter struct {
	initialized bool
	prev        stats
	g           *workGranterImpl
	b           *strings.Builder
}

func (p *deltaStatsPrinter) print() {
	latest := cloneStats(p.g.mu.stats)
	if !p.initialized {
		p.prev = latest
		p.initialized = true
		return
	}
	deltaStats := subtractStats(latest, p.prev)
	p.prev = latest
	fmt.Fprintf(p.b, "delta stats:\n")
	fmt.Fprintf(p.b, "  cpu-forecast (millis): %d\n", deltaStats.cumCPUForecast/1e6)
	var stores []roachpb.StoreID
	for storeID := range deltaStats.cumDiskForecast {
		stores = append(stores, storeID)
	}
	slices.Sort(stores)
	for _, storeID := range stores {
		rate := deltaStats.cumDiskForecast[storeID]
		fmt.Fprintf(p.b, "  disk-forecast (store=%d): %s/s\n", storeID, humanize.IBytes(uint64(rate)))
	}
	for cat, state := range deltaStats.category {
		fmt.Fprintf(p.b, "  category: %d\n", cat)
		fmt.Fprintf(p.b, "    cpu (millis): obs: %d est: %d work-wall: %d\n",
			state.cpu.observed/1e6, state.cpu.estimated/1e6, state.cpu.workWallNanos/1e6)
		var stores []roachpb.StoreID
		for storeID := range state.diskWrite {
			stores = append(stores, storeID)
		}
		slices.Sort(stores)
		for _, storeID := range stores {
			stats := state.diskWrite[storeID]
			fmt.Fprintf(p.b, "    disk-write (store=%d): obs: %s est: %s work-wall: %d\n",
				storeID, humanize.IBytes(uint64(state.diskWrite[storeID].observed)),
				humanize.IBytes(uint64(state.diskWrite[storeID].estimated)),
				stats.workWallNanos/1e6)
		}
	}
}

func cloneStats(s stats) stats {
	rv := s
	rv.cumDiskForecast = map[roachpb.StoreID]int64{}
	for storeID, rate := range s.cumDiskForecast {
		rv.cumDiskForecast[storeID] = rate
	}
	for i := range rv.category {
		rv.category[i].diskWrite = map[roachpb.StoreID]*cumulativeResourceStats{}
		for storeID, stats := range s.category[i].diskWrite {
			rv.category[i].diskWrite[storeID] = &cumulativeResourceStats{
				observed:      stats.observed,
				estimated:     stats.estimated,
				workWallNanos: stats.workWallNanos,
			}
		}
	}
	return rv
}

func subtractStats(a, b stats) stats {
	a = cloneStats(a)
	a.cumCPUForecast = a.cumCPUForecast - b.cumCPUForecast
	for storeID, rate := range a.cumDiskForecast {
		a.cumDiskForecast[storeID] = rate - b.cumDiskForecast[storeID]
	}
	for i := range a.category {
		a.category[i].cpu.observed = a.category[i].cpu.observed - b.category[i].cpu.observed
		a.category[i].cpu.estimated = a.category[i].cpu.estimated - b.category[i].cpu.estimated
		a.category[i].cpu.workWallNanos = a.category[i].cpu.workWallNanos - b.category[i].cpu.workWallNanos
		for storeID, stats := range a.category[i].diskWrite {
			bstats := b.category[i].diskWrite[storeID]
			if bstats == nil {
				continue
			}
			stats.observed = stats.observed - b.category[i].diskWrite[storeID].observed
			stats.estimated = stats.estimated - b.category[i].diskWrite[storeID].estimated
			stats.workWallNanos = stats.workWallNanos - b.category[i].diskWrite[storeID].workWallNanos
		}
	}
	return a
}

func TestGranterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var st *cluster.Settings
	var stopper *stop.Stopper
	var manualTime noTickManualTime
	var g *workGranterImpl
	var requesterStates testRequesterStates
	var statsProvider testResourceStatsProvider
	var goMaxProcs int
	var statsPrinter deltaStatsPrinter
	var b strings.Builder
	const testDefaultGoMaxProcs = 8
	const testDefaultDiskBandwidth = 125 << 20
	initFunc := func() {
		if stopper != nil {
			stopper.Stop(ctx)
		}
		st = cluster.MakeTestingClusterSettings()
		stopper = stop.NewStopper()
		manualTime = noTickManualTime{timeutil.NewManualTime(timeutil.Unix(0, 0))}
		requesterStates = testRequesterStates{
			t:          manualTime.ManualTime,
			requesters: map[WorkCategoryAndStore]*testRequesterState{},
		}
		g = newLongWorkGranterImpl(st, stopper, &manualTime, requesterStates.newUsageReporter,
			func() int {
				return goMaxProcs
			})
		g.deterministicForTesting = true
		statsProvider = testResourceStatsProvider{StatsForResources{
			CPU:  ResourceStats{ProvisionedRate: testDefaultGoMaxProcs * 1e9},
			Disk: map[roachpb.StoreID]ResourceStats{}}}
		goMaxProcs = testDefaultGoMaxProcs
		statsPrinter = deltaStatsPrinter{g: g, b: &b}
		b.Reset()
	}
	allowedWithoutPermissionStr := func() string {
		allowedCompactionsMult := compactionsAllowedWithoutPermissionMultiplier.Get(&st.SV)
		allowedSnapshotsMult := receiveSnapshotAllowedWithoutPermissionMultiplier.Get(&st.SV)
		allowedCompactionsStr := "unlimited"
		allowedSnapshotsStr := "unlimited"
		if allowedCompactionsMult > 0 {
			allowedCompactionsStr = fmt.Sprintf("%.2f", allowedCompactionsMult*float64(goMaxProcs))
		}
		if allowedSnapshotsMult > 0 {
			allowedSnapshotsStr = fmt.Sprintf("%.2f", allowedSnapshotsMult*float64(goMaxProcs))
		}
		return fmt.Sprintf("compactions: %s, snapshots: %s", allowedCompactionsStr, allowedSnapshotsStr)
	}
	granterStateStr := func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		fmt.Fprintf(&b, "granter-state:\n")
		fmt.Fprintf(&b, "  allowed-without-permission: compactions=%d, snapshots=%d\n",
			g.mu.allowedWithoutPermission[PebbleCompaction],
			g.mu.allowedWithoutPermission[SnapshotRecv])
		fmt.Fprintf(&b, "  rate-threshold: cpu=%dms/s", g.mu.rateThresholds.CPU/1e6)
		if len(g.mu.rateThresholds.DiskWrite) > 0 {
			fmt.Fprintf(&b, ", disk-write:")
			var stores []roachpb.StoreID
			for storeID := range g.mu.rateThresholds.DiskWrite {
				stores = append(stores, storeID)
			}
			slices.Sort(stores)
			for _, storeID := range stores {
				rate := g.mu.rateThresholds.DiskWrite[storeID]
				fmt.Fprintf(&b, " s%d: %s/s", storeID, humanize.IBytes(uint64(rate)))
			}
		}
		fmt.Fprintf(&b, "\n")
		if g.mu.initRateForecast {
			fmt.Fprintf(&b, "  rate-forecast: cpu=%dms/s", g.mu.intRateForecast.CPU/1e6)
			if len(g.mu.intRateForecast.DiskWrite) > 0 {
				fmt.Fprintf(&b, ", disk-write:")
				var stores []roachpb.StoreID
				for storeID := range g.mu.intRateForecast.DiskWrite {
					stores = append(stores, storeID)
				}
				slices.Sort(stores)
				for _, storeID := range stores {
					rate := g.mu.intRateForecast.DiskWrite[storeID]
					fmt.Fprintf(&b, " s%d: %s/s", storeID, humanize.IBytes(uint64(rate)))
				}
			}
		}
		fmt.Fprintf(&b, "\n")
		var works []WorkCategoryAndStore
		for wcAndStore := range g.mu.workRateEstimate {
			works = append(works, wcAndStore)
		}
		slices.SortFunc(works, func(a, b WorkCategoryAndStore) int {
			return cmp.Or(cmp.Compare(a.Category, b.Category), cmp.Compare(a.Store, b.Store))
		})
		for _, wcAndStore := range works {
			estimate := g.mu.workRateEstimate[wcAndStore]
			if estimate == (workResourceUsageRate{}) {
				continue
			}
			fmt.Fprintf(&b, "  rate-estimate: %+v: cpu=%dms/s write: %s/s\n", wcAndStore,
				estimate[cpu]/1e6, humanize.IBytes(uint64(estimate[diskWrite])))
		}
	}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "granter"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				initFunc()
				return ""

			case "register-requester":
				category := scanCategory(t, d)
				storeID := scanStoreID(t, d)
				wcAndStore := WorkCategoryAndStore{category, storeID}
				requester := &testRequesterState{
					parent:                   &requesterStates,
					wcAndStore:               wcAndStore,
					allowedWithoutPermission: 1,
					score:                    WorkScore{},
					acceptGrant:              false,
					reporters:                map[workID]*testUsageReporter{},
					b:                        &b,
				}
				requesterStates.requesters[wcAndStore] = requester
				g.RegisterRequester(wcAndStore, requester, 1)
				return ""

			case "unregister-requester":
				category := scanCategory(t, d)
				storeID := scanStoreID(t, d)
				wcAndStore := WorkCategoryAndStore{category, storeID}
				requester := requesterStates.requesters[wcAndStore]
				delete(requesterStates.requesters, wcAndStore)
				g.UnregisterRequester(wcAndStore)
				var reporters []*testUsageReporter
				for _, reporter := range requester.reporters {
					reporters = append(reporters, reporter)
				}
				slices.SortFunc(reporters, func(a, b *testUsageReporter) int {
					return cmp.Compare(a.id, b.id)
				})
				for _, reporter := range reporters {
					reporter.WorkDone()
				}
				str := b.String()
				b.Reset()
				return str

			case "try-get":
				category := scanCategory(t, d)
				storeID := scanStoreID(t, d)
				wcAndStore := WorkCategoryAndStore{category, storeID}
				requester := requesterStates.requesters[wcAndStore]
				ok, reporter := g.TryGet(wcAndStore)
				bStr := b.String()
				b.Reset()
				if ok {
					require.NotNil(t, reporter)
					r := reporter.(*testUsageReporter)
					requester.reporters[r.id] = r
					return fmt.Sprintf("%ssuccess: id=%d", bStr, r.id)
				} else {
					require.Nil(t, reporter)
					return fmt.Sprintf("%sfailure", bStr)
				}

			case "set-requester-state":
				category := scanCategory(t, d)
				storeID := scanStoreID(t, d)
				wcAndStore := WorkCategoryAndStore{category, storeID}
				requester := requesterStates.requesters[wcAndStore]
				if d.HasArg("allowed-without-permission") {
					d.ScanArgs(t, "allowed-without-permission", &requester.allowedWithoutPermission)
				}
				if d.HasArg("score") {
					var scoreStr string
					d.ScanArgs(t, "score", &scoreStr)
					vals := strings.Split(scoreStr, ",")
					require.Equal(t, 2, len(vals))
					switch vals[0] {
					case "false":
						requester.score.Optional = false
					case "true":
						requester.score.Optional = true
					default:
						panic("unknown bool value: " + vals[0])
					}
					var err error
					requester.score.Score, err = strconv.ParseFloat(vals[1], 64)
					require.NoError(t, err)
				}
				if d.HasArg("accept-grant") {
					d.ScanArgs(t, "accept-grant", &requester.acceptGrant)
				}
				return fmt.Sprintf("allowed-without-permission=%d, score=%+v, accept-grant=%t",
					requester.allowedWithoutPermission, requester.score, requester.acceptGrant)

			case "set-work-usage":
				var id uint64
				d.ScanArgs(t, "id", &id)
				for _, requester := range requesterStates.requesters {
					for _, reporter := range requester.reporters {
						if reporter.id == workID(id) {
							if d.HasArg("start-millis") {
								var start int
								d.ScanArgs(t, "start-millis", &start)
								reporter.usage.startTimeUnixNanos = int64(start) * 1e6
							}
							if d.HasArg("end-millis") {
								var end int
								d.ScanArgs(t, "end-millis", &end)
								reporter.usage.endTimeUnixNanos = int64(end) * 1e6
							}
							if d.HasArg("cpu-millis") {
								var cpuMillis int
								d.ScanArgs(t, "cpu-millis", &cpuMillis)
								reporter.usage.cumulative[cpu] += int64(cpuMillis) * 1e6
							}
							if d.HasArg("write-bytes") {
								var writeBytesStr string
								d.ScanArgs(t, "write-bytes", &writeBytesStr)
								writeBytes, err := humanize.ParseBytes(writeBytesStr)
								require.NoError(t, err)
								reporter.usage.cumulative[diskWrite] += int64(writeBytes)
							}
							return fmt.Sprintf("start: %d, end: %d, cpu: %d, write: %d",
								reporter.usage.startTimeUnixNanos/1e6, reporter.usage.endTimeUnixNanos/1e6,
								reporter.usage.cumulative[cpu]/1e6, reporter.usage.cumulative[diskWrite])
						}
					}
				}
				return fmt.Sprintf("work not found: %d", id)

			case "work-done":
				var id uint64
				d.ScanArgs(t, "id", &id)
				for _, requester := range requesterStates.requesters {
					for _, reporter := range requester.reporters {
						if reporter.id == workID(id) {
							reporter.WorkDone()
							delete(requester.reporters, reporter.id)
							granterStateStr()
							str := b.String()
							b.Reset()
							return str
						}
					}
				}
				return fmt.Sprintf("work not found: %d", id)

			case "register-stats-provider":
				r := metric.NewRegistry()
				storeRegistries := map[roachpb.StoreID]*metric.Registry{}
				storeRegistries[1] = r
				storeRegistries[2] = r
				g.RegisterStatsProvider(&statsProvider, r, storeRegistries)
				return ""

			case "tick":
				manualTime.Advance(tickInterval)
				fmt.Fprintf(&b, "time=%dms\n", manualTime.Now().UnixMilli())
				g.tick()
				granterStateStr()
				statsPrinter.print()
				str := b.String()
				b.Reset()
				return str

			case "set-utilization":
				var threshold float64
				d.ScanArgs(t, "threshold", &threshold)
				utilizationThreshold.Override(ctx, &st.SV, threshold)
				return ""

			case "set-compactions-allowed":
				var multiplier float64
				d.ScanArgs(t, "multiplier", &multiplier)
				compactionsAllowedWithoutPermissionMultiplier.Override(ctx, &st.SV, multiplier)
				return allowedWithoutPermissionStr()

			case "set-snapshots-allowed":
				var multiplier float64
				d.ScanArgs(t, "multiplier", &multiplier)
				receiveSnapshotAllowedWithoutPermissionMultiplier.Override(ctx, &st.SV, multiplier)
				return allowedWithoutPermissionStr()

			case "set-go-max-procs":
				var procs int
				d.ScanArgs(t, "procs", &procs)
				goMaxProcs = procs
				return allowedWithoutPermissionStr()

			case "set-stats":
				if d.HasArg("cpu-millis") {
					var cpuMillis int
					d.ScanArgs(t, "cpu-millis", &cpuMillis)
					statsProvider.stats.CPU.Used += int64(cpuMillis) * 1e6
				}
				if d.HasArg("cpu-provisioned-millis") {
					var cpuProvisionedMillis int
					d.ScanArgs(t, "cpu-provisioned-millis", &cpuProvisionedMillis)
					statsProvider.stats.CPU.ProvisionedRate = int64(cpuProvisionedMillis) * 1e6
				}
				if d.HasArg("stores") {
					var storesStr string
					d.ScanArgs(t, "stores", &storesStr)
					vals := strings.Split(storesStr, ",")
					for _, val := range vals {
						parts := strings.Split(val, ":")
						require.Equal(t, 2, len(parts))
						storeID, err := strconv.Atoi(parts[0])
						require.NoError(t, err)
						writeBytes, err := humanize.ParseBytes(parts[1])
						require.NoError(t, err)
						diskStats := statsProvider.stats.Disk[roachpb.StoreID(storeID)]
						diskStats.ProvisionedRate = testDefaultDiskBandwidth
						diskStats.Used += int64(writeBytes)
						statsProvider.stats.Disk[roachpb.StoreID(storeID)] = diskStats
					}
				}
				return fmt.Sprintf("%+v", statsProvider.stats)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	if stopper != nil {
		stopper.Stop(ctx)
	}
}

func scanCategory(t *testing.T, d *datadriven.TestData) WorkCategory {
	var catStr string
	d.ScanArgs(t, "category", &catStr)
	switch catStr {
	case "compaction":
		return PebbleCompaction
	case "snapshot":
		return SnapshotRecv
	default:
		panic("unknown category: " + catStr)
	}
}

func scanStoreID(t *testing.T, d *datadriven.TestData) roachpb.StoreID {
	var storeID int
	d.ScanArgs(t, "store-id", &storeID)
	return roachpb.StoreID(storeID)
}
