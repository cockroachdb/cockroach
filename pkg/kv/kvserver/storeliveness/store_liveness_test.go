// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestStoreLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(
		t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
			ctx := context.Background()
			storeID := slpb.StoreIdent{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)}
			engine := NewTestEngine(storeID)
			defer engine.Close()
			settings := clustersettings.MakeTestingClusterSettings()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			manual := timeutil.NewManualTime(timeutil.Unix(1, 0))
			clock := hlc.NewClockForTesting(manual)
			sender := testMessageSender{}
			sm := NewSupportManager(storeID, engine, Options{}, settings, stopper, clock, &sender, nil)
			require.NoError(t, sm.onRestart(ctx))
			datadriven.RunTest(
				t, path, func(t *testing.T, d *datadriven.TestData) string {
					switch d.Cmd {
					case "mark-idle-stores":
						sm.requesterStateHandler.markIdleStores(ctx)
						return ""

					case "support-from":
						remoteID := parseStoreID(t, d, "node-id", "store-id")
						epoch, timestamp := sm.SupportFrom(remoteID)
						return fmt.Sprintf("epoch: %+v, expiration: %+v", epoch, timestamp)

					case "support-for":
						remoteID := parseStoreID(t, d, "node-id", "store-id")
						epoch, supported := sm.SupportFor(remoteID)
						return fmt.Sprintf("epoch: %+v, support provided: %v", epoch, supported)

					case "send-heartbeats":
						now := parseTimestamp(t, d, "now")
						manual.AdvanceTo(now.GoTime())
						sm.options.SupportDuration = parseDuration(t, d, "support-duration")
						sm.maybeAddStores(ctx)
						sm.sendHeartbeats(ctx)
						heartbeats := sender.drainSentMessages()
						return fmt.Sprintf("heartbeats:\n%s", printMsgs(heartbeats))

					case "handle-messages":
						msgs := parseMsgs(t, d, storeID)
						sm.handleMessages(ctx, msgs)
						responses := sender.drainSentMessages()
						if len(responses) > 0 {
							return fmt.Sprintf("responses:\n%s", printMsgs(responses))
						} else {
							return ""
						}

					case "withdraw-support":
						now := parseTimestamp(t, d, "now")
						manual.AdvanceTo(now.GoTime())
						sm.withdrawSupport(ctx)
						return ""

					case "restart":
						now := parseTimestamp(t, d, "now")
						gracePeriod := parseDuration(t, d, "grace-period")
						o := Options{SupportWithdrawalGracePeriod: gracePeriod}
						sm = NewSupportManager(
							storeID, engine, o, settings, stopper, clock, &sender, nil,
						)
						manual.AdvanceTo(now.GoTime())
						require.NoError(t, sm.onRestart(ctx))
						return ""

					case "error-on-write":
						var errorOnWrite bool
						d.ScanArgs(t, "on", &errorOnWrite)
						engine.SetErrorOnWrite(errorOnWrite)
						return ""

					case "debug-requester-state":
						var sortedSupportMap []string
						for _, support := range sm.requesterStateHandler.requesterState.supportFrom {
							sortedSupportMap = append(
								sortedSupportMap, fmt.Sprintf("%+v", support.state),
							)
						}
						slices.Sort(sortedSupportMap)
						return fmt.Sprintf(
							"meta:\n%+v\nsupport from:\n%+v",
							sm.requesterStateHandler.requesterState.meta,
							strings.Join(sortedSupportMap, "\n"),
						)

					case "debug-supporter-state":
						var sortedSupportMap []string
						for _, support := range sm.supporterStateHandler.supporterState.supportFor {
							sortedSupportMap = append(sortedSupportMap, fmt.Sprintf("%+v", support))
						}
						slices.Sort(sortedSupportMap)
						return fmt.Sprintf(
							"meta:\n%+v\nsupport for:\n%+v",
							sm.supporterStateHandler.supporterState.meta,
							strings.Join(sortedSupportMap, "\n"),
						)

					case "debug-metrics":
						return fmt.Sprintf(
							"HeartbeatSuccess: %d, HeartbeatFailure: %d\n"+
								"MessageHandleSuccess: %d, MessageHandleFailure: %d\n"+
								"SupportWithdrawSuccess: %d, SupportWithdrawFailure: %d\n"+
								"SupportFromStores: %d, SupportForStores: %d",
							sm.metrics.HeartbeatSuccesses.Count(),
							sm.metrics.HeartbeatFailures.Count(),
							sm.metrics.MessageHandleSuccesses.Count(),
							sm.metrics.MessageHandleFailures.Count(),
							sm.metrics.SupportWithdrawSuccesses.Count(),
							sm.metrics.SupportWithdrawFailures.Count(),
							sm.metrics.SupportFromStores.Value(),
							sm.metrics.SupportForStores.Value(),
						)
					default:
						return fmt.Sprintf("unknown command: %s", d.Cmd)
					}
				},
			)
		},
	)
}

func printMsgs(msgs []slpb.Message) string {
	var sortedMsgs []string
	for _, msg := range msgs {
		sortedMsgs = append(sortedMsgs, fmt.Sprintf("%+v", msg))
	}
	// Sort the messages for a deterministic output.
	slices.Sort(sortedMsgs)
	return strings.Join(sortedMsgs, "\n")
}

func parseStoreID(
	t *testing.T, d *datadriven.TestData, nodeStr string, storeStr string,
) slpb.StoreIdent {
	var nodeID int64
	d.ScanArgs(t, nodeStr, &nodeID)
	var storeID int64
	d.ScanArgs(t, storeStr, &storeID)
	return slpb.StoreIdent{
		NodeID:  roachpb.NodeID(nodeID),
		StoreID: roachpb.StoreID(storeID),
	}
}

func parseTimestamp(t *testing.T, d *datadriven.TestData, name string) hlc.Timestamp {
	var wallTimeSecs int64
	d.ScanArgs(t, name, &wallTimeSecs)
	wallTime := wallTimeSecs * int64(time.Second)
	return hlc.Timestamp{WallTime: wallTime}
}

func parseDuration(t *testing.T, d *datadriven.TestData, name string) time.Duration {
	var durationStr string
	d.ScanArgs(t, name, &durationStr)
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		t.Errorf("can't parse duration %s; error: %v", durationStr, err)
	}
	return duration
}

func parseMsgs(t *testing.T, d *datadriven.TestData, storeIdent slpb.StoreIdent) []*slpb.Message {
	var msgs []*slpb.Message
	lines := strings.Split(d.Input, "\n")
	for _, line := range lines {
		var err error
		d.Cmd, d.CmdArgs, err = datadriven.ParseLine(line)
		if err != nil {
			d.Fatalf(t, "error parsing message: %v", err)
		}
		if d.Cmd != "msg" {
			d.Fatalf(t, "expected \"msg\", found %s", d.Cmd)
		}
		var msgTypeStr string
		d.ScanArgs(t, "type", &msgTypeStr)
		var msgType slpb.MessageType
		switch msgTypeStr {
		case slpb.MsgHeartbeat.String():
			msgType = slpb.MsgHeartbeat
		case slpb.MsgHeartbeatResp.String():
			msgType = slpb.MsgHeartbeatResp
		default:
			d.Fatalf(t, "unexpected \"type\", found %s", msgTypeStr)
		}
		remoteID := parseStoreID(t, d, "from-node-id", "from-store-id")
		var epoch int64
		d.ScanArgs(t, "epoch", &epoch)
		expiration := parseTimestamp(t, d, "expiration")
		msg := &slpb.Message{
			Type:       msgType,
			From:       remoteID,
			To:         storeIdent,
			Epoch:      slpb.Epoch(epoch),
			Expiration: expiration,
		}
		msgs = append(msgs, msg)
	}
	return msgs
}
