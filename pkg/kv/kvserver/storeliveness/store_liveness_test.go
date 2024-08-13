// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestStoreLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	storeID := slpb.StoreIdent{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)}

	datadriven.Walk(
		t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
			engine := storage.NewDefaultInMemForTesting()
			defer engine.Close()
			ss := newSupporterStateHandler()
			rs := newRequesterStateHandler()
			if err := onRestart(ctx, rs, ss, engine); err != nil {
				t.Errorf("persisting data while restarting failed: %v", err)
			}
			datadriven.RunTest(
				t, path, func(t *testing.T, d *datadriven.TestData) string {
					switch d.Cmd {
					case "add-store":
						remoteID := parseStoreID(t, d, "node-id", "store-id")
						rs.addStore(remoteID)
						return ""

					case "remove-store":
						remoteID := parseStoreID(t, d, "node-id", "store-id")
						rs.removeStore(remoteID)
						return ""

					case "support-from":
						remoteID := parseStoreID(t, d, "node-id", "store-id")
						supportState, _ := rs.getSupportFrom(remoteID)
						return fmt.Sprintf("requester state: %+v", supportState)

					case "support-for":
						remoteID := parseStoreID(t, d, "node-id", "store-id")
						supportState := ss.getSupportFor(remoteID)
						return fmt.Sprintf("supporter state: %+v", supportState)

					case "send-heartbeats":
						now := parseTimestamp(t, d, "now")
						var interval string
						d.ScanArgs(t, "liveness-interval", &interval)
						livenessInterval, err := time.ParseDuration(interval)
						if err != nil {
							t.Errorf("can't parse liveness interval duration %s; error: %v", interval, err)
						}
						rsfu := rs.checkOutUpdate()
						heartbeats := rsfu.getHeartbeatsToSend(storeID, now, livenessInterval)
						if err = rsfu.write(ctx, engine); err != nil {
							t.Errorf("writing requester state failed: %v", err)
						}
						rs.checkInUpdate(rsfu)
						return fmt.Sprintf("heartbeats:\n%s", printMsgs(heartbeats))

					case "handle-messages":
						msgs := parseMsgs(t, d, storeID)
						var responses []slpb.Message
						rsfu := rs.checkOutUpdate()
						ssfu := ss.checkOutUpdate()
						for _, msg := range msgs {
							switch msg.Type {
							case slpb.MsgHeartbeat:
								responses = append(responses, ssfu.handleHeartbeat(msg))
							case slpb.MsgHeartbeatResp:
								rsfu.handleHeartbeatResponse(msg)
							default:
								log.Errorf(context.Background(), "unexpected message type: %v", msg.Type)
							}
						}
						if err := rsfu.write(ctx, engine); err != nil {
							t.Errorf("writing requester state failed: %v", err)
						}
						if err := ssfu.write(ctx, engine); err != nil {
							t.Errorf("writing supporter state failed: %v", err)
						}
						rs.checkInUpdate(rsfu)
						ss.checkInUpdate(ssfu)
						if len(responses) > 0 {
							return fmt.Sprintf("responses:\n%s", printMsgs(responses))
						} else {
							return ""
						}

					case "withdraw-support":
						now := parseTimestamp(t, d, "now")
						ssfu := ss.checkOutUpdate()
						ssfu.withdrawSupport(hlc.ClockTimestamp(now))
						if err := ssfu.write(ctx, engine); err != nil {
							t.Errorf("writing supporter state failed: %v", err)
						}
						ss.checkInUpdate(ssfu)
						return ""

					case "restart":
						ss = newSupporterStateHandler()
						rs = newRequesterStateHandler()
						if err := onRestart(ctx, rs, ss, engine); err != nil {
							t.Errorf("persisting data while restarting failed: %v", err)
						}
						return ""

					case "debug-requester-state":
						return fmt.Sprintf(
							"meta:\n%+v\nsupport from:\n%+v", rs.requesterState.meta,
							printSupportMap(rs.requesterState.supportFrom),
						)

					case "debug-supporter-state":
						return fmt.Sprintf(
							"meta:\n%+v\nsupport for:\n%+v", ss.supporterState.meta,
							printSupportMap(ss.supporterState.supportFor),
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

func printSupportMap(m map[slpb.StoreIdent]slpb.SupportState) string {
	var sortedSupportMap []string
	for _, support := range m {
		sortedSupportMap = append(sortedSupportMap, fmt.Sprintf("%+v", support))
	}
	slices.Sort(sortedSupportMap)
	return strings.Join(sortedSupportMap, "\n")
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

func parseTimestamp(t *testing.T, d *datadriven.TestData, timeStr string) hlc.Timestamp {
	var wallTimeSecs int64
	d.ScanArgs(t, timeStr, &wallTimeSecs)
	wallTime := wallTimeSecs * int64(time.Second)
	return hlc.Timestamp{WallTime: wallTime}
}

func parseMsgs(t *testing.T, d *datadriven.TestData, storeIdent slpb.StoreIdent) []slpb.Message {
	var msgs []slpb.Message
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
		msg := slpb.Message{
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

// TODO(mira): Move this to the SupportManager.
func onRestart(
	ctx context.Context, rs *requesterStateHandler, ss *supporterStateHandler, engine storage.Engine,
) error {
	if err := ss.read(ctx, engine); err != nil {
		return err
	}
	if err := rs.read(ctx, engine); err != nil {
		return err
	}
	rsfu := rs.checkOutUpdate()
	rsfu.incrementMaxEpoch()
	if err := rsfu.write(ctx, engine); err != nil {
		return err
	}
	rs.checkInUpdate(rsfu)
	return nil
}
