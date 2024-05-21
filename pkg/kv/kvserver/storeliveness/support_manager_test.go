package storeliveness

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
)

type testHeartbeatSender struct {
	heartbeats []storelivenesspb.HeartbeatRequest
}

func (ths *testHeartbeatSender) flushHeartbeats() string {
	heartbeatsStr := fmt.Sprintf("heartbeats:")
	for _, h := range ths.heartbeats {
		heartbeatsStr = fmt.Sprintf("%s\n%+v", heartbeatsStr, h)
	}
	ths.heartbeats = []storelivenesspb.HeartbeatRequest{}
	return heartbeatsStr
}

func (ths *testHeartbeatSender) SendHeartbeat(
	req *storelivenesspb.HeartbeatRequest,
) (*storelivenesspb.HeartbeatResponse, error) {
	ths.heartbeats = append(ths.heartbeats, *req)
	res := &storelivenesspb.HeartbeatResponse{HeartbeatAck: true}
	return res, nil
}

type testCallbackScheduler struct {
	t   *timeutil.ManualTime
	cbs []testCallbackAndTime
}

type testCallbackAndTime struct {
	runNanos int64
	f        *func()
}

func (tcs *testCallbackScheduler) registerCallback(
	debugName string, f func(),
) callbackSchedulerHandle {
	return &testCallbackSchedulerHandle{
		scheduler: tcs,
		f:         f,
	}
}

func (tcs *testCallbackScheduler) tryRun() {
	nowNanos := tcs.t.Now().UnixNano()
	var toRun []func()
	tcs.cbs = slices.DeleteFunc(tcs.cbs, func(a testCallbackAndTime) bool {
		if a.runNanos <= nowNanos {
			toRun = append(toRun, *a.f)
			return true
		}
		return false
	})
	for _, f := range toRun {
		f()
	}
}

type testCallbackSchedulerHandle struct {
	scheduler *testCallbackScheduler
	f         func()
}

func (h *testCallbackSchedulerHandle) runCallbackAfterDuration(d time.Duration) {
	h.scheduler.cbs = slices.DeleteFunc(h.scheduler.cbs, func(a testCallbackAndTime) bool {
		return a.f == &h.f
	})
	var runNanos int64
	if d == time.Duration(math.MaxInt64) {
		runNanos = math.MaxInt64
	} else {
		runNanos = h.scheduler.t.Now().UnixNano() + int64(d)
	}
	h.scheduler.cbs = append(h.scheduler.cbs, testCallbackAndTime{
		runNanos: runNanos,
		f:        &h.f,
	})
}

func (h *testCallbackSchedulerHandle) unregister() {
	h.scheduler.cbs = slices.DeleteFunc(h.scheduler.cbs, func(a testCallbackAndTime) bool {
		return a.f == &h.f
	})
}

func TestManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	manualTime := timeutil.NewManualTime(timeutil.Unix(1, 0))
	clock := hlc.NewClock(manualTime, time.Hour*1000, time.Hour*1000)
	tcs := &testCallbackScheduler{t: manualTime}
	ths := &testHeartbeatSender{
		heartbeats: []storelivenesspb.HeartbeatRequest{},
	}
	opt := Options{
		Clock:             clock,
		HeartbeatInterval: 1 * time.Second,
		callbackScheduler: tcs,
	}
	nodeID := roachpb.NodeID(1)
	storeID := roachpb.StoreID(1)
	id := storelivenesspb.StoreIdent{NodeID: nodeID, StoreID: storeID}
	m := NewSupportManager(id, opt, stopper, ths)

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "support_manager"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "add-store":
				var remoteNodeID int64
				d.ScanArgs(t, "node-id", &remoteNodeID)
				var remoteStoreID int64
				d.ScanArgs(t, "store-id", &remoteStoreID)
				remoteID := storelivenesspb.StoreIdent{
					NodeID:  roachpb.NodeID(remoteNodeID),
					StoreID: roachpb.StoreID(remoteStoreID),
				}
				m.addStore(remoteID)
				return ths.flushHeartbeats()

			case "remove-store":
				var remoteNodeID int64
				d.ScanArgs(t, "node-id", &remoteNodeID)
				var remoteStoreID int64
				d.ScanArgs(t, "store-id", &remoteStoreID)
				remoteID := storelivenesspb.StoreIdent{
					NodeID:  roachpb.NodeID(remoteNodeID),
					StoreID: roachpb.StoreID(remoteStoreID),
				}
				m.removeStore(remoteID)
				return ths.flushHeartbeats()

			case "set-time":
				var nowMilli int64
				d.ScanArgs(t, "now-milli", &nowMilli)
				manualTime.MustAdvanceTo(time.UnixMilli(nowMilli))
				tcs.tryRun()
				clockTime := fmt.Sprintf("hlc: %+v", clock.NowAsClockTimestamp())
				heartbeats := ths.flushHeartbeats()
				return clockTime + "\n" + heartbeats

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
