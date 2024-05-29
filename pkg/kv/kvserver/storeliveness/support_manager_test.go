package storeliveness

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
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

func TestManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	manualTime := timeutil.NewManualTime(timeutil.Unix(1, 0))
	clock := hlc.NewClock(manualTime, time.Hour*1000, time.Hour*1000)
	cfg := kvserver.TestStoreConfig(clock)
	opt := Options{
		Clock:                    clock,
		HeartbeatInterval:        1 * time.Second,
		SupportExpiryInterval:    1 * time.Second,
		ResponseHandlingInterval: 1 * time.Second,
	}
	nodeID := roachpb.NodeID(1)
	storeID := roachpb.StoreID(1)
	id := storelivenesspb.StoreIdent{NodeID: nodeID, StoreID: storeID}
	transport := NewDummyTransport(cfg.Settings, cfg.AmbientCtx.Tracer, cfg.Clock)
	m := NewSupportManager(id, opt, stopper, transport)

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
				return ""

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
				return ""
			case "set-time":
				var nowMilli int64
				d.ScanArgs(t, "now-milli", &nowMilli)
				manualTime.MustAdvanceTo(time.UnixMilli(nowMilli))
				clockTime := fmt.Sprintf("hlc: %+v", clock.NowAsClockTimestamp())
				heartbeats := ""
				return clockTime + "\n" + heartbeats

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
