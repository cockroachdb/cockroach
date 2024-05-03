// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package dme_liveness

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/dme_liveness/dme_livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

type testMessageSender struct {
	b          strings.Builder
	ssProvider SupportStateForPropagationProvider
	hhri       HandleHeartbeatResponseInterface

	nextResponses map[dme_livenesspb.StoreIdentifier]nextResponse
}

type nextResponse struct {
	// Default initialized struct provides an ack with 0 responderTime.

	// If doNotRepond is true, a response will not be provided.
	doNotRespond bool
	// Nacks cause an immediate heartbeat. numNacks configures the number of
	// nacks to send, which is decremented with every nack.
	numNacks      int
	responderTime hlc.ClockTimestamp
}

func (ms *testMessageSender) SetSupportStateProvider(
	ssProvider SupportStateForPropagationProvider,
) {
	ms.ssProvider = ssProvider
}

func (ms *testMessageSender) SetHandleHeartbeatResponseInterface(
	hhri HandleHeartbeatResponseInterface,
) {
	ms.hhri = hhri
}

func (ms *testMessageSender) SendHeartbeat(
	header dme_livenesspb.Header, msg dme_livenesspb.Heartbeat,
) {
	nr := ms.nextResponses[header.To]
	ack := true
	if !nr.doNotRespond && nr.numNacks > 0 {
		ack = false
		nr.numNacks--
	}
	ms.nextResponses[header.To] = nr
	fmt.Fprintf(&ms.b, " SendHeartbeat: %+v, %+v", header, msg)
	if !nr.doNotRespond {
		fmt.Fprintf(&ms.b, " response: ack=%t rt=%s\n", ack, nr.responderTime)
		err := ms.hhri.HandleHeartbeatResponse(header.To, msg, nr.responderTime, ack)
		if err != nil {
			fmt.Fprintf(&ms.b, "   response handling err: %s\n", err.Error())
		}
	} else {
		fmt.Fprintf(&ms.b, "\n")
	}
}

func (ms *testMessageSender) PropagateLocalSupportStateSoon() {
	ss := ms.ssProvider.LocalSupportState()
	fmt.Fprintf(&ms.b, " PLSSS: t=%s\n", ss.StateTime.String())
	fmt.Fprintf(&ms.b, " for-self-by: %+v\n", ss.ForOtherBy.For)
	slices.SortFunc(ss.ForOtherBy.Support, func(a, b dme_livenesspb.SupportForOtherBySingle) int {
		if a.By.NodeID != b.By.NodeID {
			return cmp.Compare(a.By.NodeID, b.By.NodeID)
		}
		if a.By.StoreID != b.By.StoreID {
			return cmp.Compare(a.By.StoreID, b.By.StoreID)
		}
		return 0
	})
	for _, s := range ss.ForOtherBy.Support {
		fmt.Fprintf(&ms.b, "  by: %+v support: %+v\n", s.By, s.Support)
	}
	fmt.Fprintf(&ms.b, " by-self-for: %+v\n", ss.ByOtherFor.By)
	slices.SortFunc(ss.ByOtherFor.Support, func(a, b dme_livenesspb.SupportByOtherForSingle) int {
		if a.For.NodeID != b.For.NodeID {
			return cmp.Compare(a.For.NodeID, b.For.NodeID)
		}
		if a.For.StoreID != b.For.StoreID {
			return cmp.Compare(a.For.StoreID, b.For.StoreID)
		}
		return 0
	})
	for _, s := range ss.ByOtherFor.Support {
		fmt.Fprintf(&ms.b, "  for: %+v support: %+v\n", s.For, s.Support)
	}
}

func (ms *testMessageSender) AddRemoteStore(store dme_livenesspb.StoreIdentifier) {
	fmt.Fprintf(&ms.b, " AddRemoteStore: %+v\n", store)
}

func (ms *testMessageSender) RemoveRemoteStore(store dme_livenesspb.StoreIdentifier) {
	fmt.Fprintf(&ms.b, " RemoveRemoteStore: %+v\n", store)
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
	st := cluster.MakeTestingClusterSettings()
	tracer := tracing.NewTracerWithOpt(context.Background(), tracing.WithClusterSettings(&st.SV))
	ambientCtx := log.MakeTestingAmbientContext(tracer)
	ctx := context.Background()
	defer stopper.Stop(ctx)
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)

	storageProvider := &StorageProviderForDMELiveness{AmbientCtx: ambientCtx, Engine: eng}
	_ = storageProvider

	manualTime := timeutil.NewManualTime(timeutil.Unix(1, 0))
	clock := hlc.NewClock(manualTime, time.Hour*1000, time.Hour*1000)
	testCallbackScheduler := &testCallbackScheduler{t: manualTime}
	nodeID := roachpb.NodeID(1)
	opt := Options{
		NodeID: nodeID,
		Clock:  clock,
		LivenessExpiryInterval: func() time.Duration {
			return time.Second
		},
		callbackSchedulerForTesting: testCallbackScheduler,
	}
	m := NewManager(opt)
	stopper.AddCloser(m)
	storeID := roachpb.StoreID(1)
	messageSender :=
		&testMessageSender{nextResponses: map[dme_livenesspb.StoreIdentifier]nextResponse{}}
	require.NoError(t, m.AddLocalStore(LocalStore{
		StoreID:         storeID,
		StorageProvider: storageProvider,
		MessageSender:   messageSender,
	}))

	descriptors := map[string]roachpb.RangeDescriptor{}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "manager"),
		func(t *testing.T, d *datadriven.TestData) string {
			printMessageSender := func(b *strings.Builder) {
				if messageSender.b.Len() == 0 {
					return
				}
				fmt.Fprintf(b, "message-sender:\n")
				fmt.Fprintf(b, "%s", messageSender.b.String())
				messageSender.b.Reset()
			}
			switch d.Cmd {
			case "descriptor":
				var name string
				d.ScanArgs(t, "name", &name)
				var desc roachpb.RangeDescriptor
				for _, l := range strings.Split(strings.TrimSpace(d.Input), "\n") {
					replicaSet := spanconfigtestutils.ParseReplicaSet(t, l)
					desc.InternalReplicas = append(desc.InternalReplicas, replicaSet.Descriptors()...)
				}
				descriptors[name] = desc
				return desc.String()

			case "has-support":
				var nowMilli int64
				d.ScanArgs(t, "now-milli", &nowMilli)
				now := hlc.ClockTimestamp{WallTime: nowMilli * int64(time.Millisecond)}
				var nodeID int64
				d.ScanArgs(t, "node-id", &nodeID)
				var storeID int64
				d.ScanArgs(t, "store-id", &storeID)
				var epoch int64
				d.ScanArgs(t, "epoch", &epoch)
				var descriptorNames []string
				d.ScanArgs(t, "descriptors", &descriptorNames)
				var descs []roachpb.RangeDescriptor
				for _, name := range descriptorNames {
					desc, ok := descriptors[name]
					if !ok {
						return fmt.Sprintf("descriptor %s not found", name)
					}
					descs = append(descs, desc)
				}
				support, expiration, err := m.HasSupport(now, dme_livenesspb.StoreIdentifier{
					NodeID:  roachpb.NodeID(nodeID),
					StoreID: roachpb.StoreID(storeID),
				}, epoch, descs)
				errStr := "<nil>"
				if err != nil {
					errStr = err.Error()
				}
				var b strings.Builder
				printMessageSender(&b)
				fmt.Fprintf(&b, "support: %s, expiration: %s, err: %s",
					support, expiration.String(), errStr)
				return b.String()

			case "lease-proposal":
				var startMilli int64
				d.ScanArgs(t, "start-milli", &startMilli)
				start := hlc.ClockTimestamp{WallTime: startMilli * int64(time.Millisecond)}
				var nodeID int64
				d.ScanArgs(t, "node-id", &nodeID)
				var storeID int64
				d.ScanArgs(t, "store-id", &storeID)
				var descriptor string
				d.ScanArgs(t, "descriptor", &descriptor)
				desc, ok := descriptors[descriptor]
				if !ok {
					return fmt.Sprintf("descriptor %s not found", descriptor)
				}
				adjustedStart, epoch, err := m.EpochAndSupportForLeaseProposal(start, dme_livenesspb.StoreIdentifier{
					NodeID:  roachpb.NodeID(nodeID),
					StoreID: roachpb.StoreID(storeID),
				}, desc)
				errStr := "<nil>"
				if err != nil {
					errStr = err.Error()
				}
				var b strings.Builder
				printMessageSender(&b)
				fmt.Fprintf(&b, "adjusted-ts: %s, epoch: %d, err: %s", adjustedStart.String(), epoch, errStr)
				return b.String()

			case "print-persistent-state":
				return storageProvider.StateForTesting()

			case "add-remote-store":
				var nodeID int64
				d.ScanArgs(t, "node-id", &nodeID)
				var storeID int64
				d.ScanArgs(t, "store-id", &storeID)
				m.AddRemoteStore(dme_livenesspb.StoreIdentifier{
					NodeID: roachpb.NodeID(nodeID), StoreID: roachpb.StoreID(storeID)})
				var b strings.Builder
				printMessageSender(&b)
				return b.String()

			case "set-next-response":
				var nodeID int64
				d.ScanArgs(t, "node-id", &nodeID)
				var storeID int64
				d.ScanArgs(t, "store-id", &storeID)
				var nr nextResponse
				if d.HasArg("do-not-respond") {
					nr.doNotRespond = true
				}
				if d.HasArg("num-nacks") {
					d.ScanArgs(t, "num-nacks", &nr.numNacks)
				}
				if d.HasArg("time-milli") {
					var tMilli int
					d.ScanArgs(t, "time-milli", &tMilli)
					nr.responderTime = hlc.ClockTimestamp{WallTime: int64(tMilli) * int64(time.Millisecond)}
				}
				storeIdentifier := dme_livenesspb.StoreIdentifier{
					NodeID: roachpb.NodeID(nodeID), StoreID: roachpb.StoreID(storeID)}
				messageSender.nextResponses[storeIdentifier] = nr
				return fmt.Sprintf("%+v: %+v", storeIdentifier, nr)

			case "handle-heartbeat":
				var nodeID int64
				d.ScanArgs(t, "node-id", &nodeID)
				var storeID int64
				d.ScanArgs(t, "store-id", &storeID)
				header := dme_livenesspb.Header{
					From: dme_livenesspb.StoreIdentifier{
						NodeID: roachpb.NodeID(nodeID), StoreID: roachpb.StoreID(storeID)},
					To: dme_livenesspb.StoreIdentifier{NodeID: 1, StoreID: 1},
				}
				var epoch int64
				d.ScanArgs(t, "epoch", &epoch)
				var senderMilli int64
				d.ScanArgs(t, "sender-milli", &senderMilli)
				senderTS := hlc.ClockTimestamp{WallTime: senderMilli * int64(time.Millisecond)}
				var endMilli int64
				d.ScanArgs(t, "end-milli", &endMilli)
				endTS := hlc.Timestamp{WallTime: endMilli * int64(time.Millisecond)}
				hb := dme_livenesspb.Heartbeat{
					Epoch:      epoch,
					SenderTime: senderTS,
					EndTime:    endTS,
				}
				ack, err := m.HandleHeartbeat(header, hb)
				var b strings.Builder
				printMessageSender(&b)
				errStr := "<nil>"
				if err != nil {
					errStr = err.Error()
				}
				fmt.Fprintf(&b, "heartbeat: ack %t err %s", ack, errStr)
				return b.String()

			case "handle-support-state":
				var nodeID int64
				d.ScanArgs(t, "node-id", &nodeID)
				var storeID int64
				d.ScanArgs(t, "store-id", &storeID)
				from := dme_livenesspb.StoreIdentifier{
					NodeID: roachpb.NodeID(nodeID), StoreID: roachpb.StoreID(storeID)}
				header := dme_livenesspb.Header{
					From: from,
					To:   dme_livenesspb.StoreIdentifier{NodeID: 1, StoreID: 1},
				}
				var supportState dme_livenesspb.SupportState
				var tMilli int64
				d.ScanArgs(t, "time-milli", &tMilli)
				supportState.StateTime = hlc.ClockTimestamp{WallTime: tMilli * int64(time.Millisecond)}
				atoi := func(s string) int {
					x, err := strconv.Atoi(s)
					require.NoError(t, err)
					return x
				}
				supportState.ForOtherBy.For = from
				supportState.ByOtherFor.By = from
				for _, l := range strings.Split(strings.TrimSpace(d.Input), "\n") {
					parts := strings.Split(l, " ")
					if len(parts) == 0 {
						continue
					}
					forOrByStr := strings.TrimSpace(parts[0])
					by := false
					switch forOrByStr {
					case "for":
					case "by":
						by = true
					default:
						return fmt.Sprintf("unknown directive %s", forOrByStr)
					}
					var storeIdentifier dme_livenesspb.StoreIdentifier
					var support dme_livenesspb.Support
					for i := 1; i < len(parts); i++ {
						part := strings.TrimSpace(parts[i])
						kvs := strings.Split(part, "=")
						switch strings.TrimSpace(kvs[0]) {
						case "store-id":
							storeIdentifier.StoreID = roachpb.StoreID(atoi(kvs[1]))
						case "node-id":
							storeIdentifier.NodeID = roachpb.NodeID(atoi(kvs[1]))
						case "epoch":
							support.Epoch = int64(atoi(kvs[1]))
						case "end-milli":
							support.EndTime = hlc.Timestamp{WallTime: int64(atoi(kvs[1])) * int64(time.Millisecond)}
						default:
							return fmt.Sprintf("unknown key %s", kvs[0])
						}
					}
					if by {
						ss := dme_livenesspb.SupportByOtherForSingle{For: storeIdentifier, Support: support}
						supportState.ByOtherFor.Support = append(supportState.ByOtherFor.Support, ss)
					} else {
						ss := dme_livenesspb.SupportForOtherBySingle{
							By:      storeIdentifier,
							Support: support,
						}
						supportState.ForOtherBy.Support = append(supportState.ForOtherBy.Support, ss)
					}
				}
				err := m.HandleSupportState(header, supportState)
				var b strings.Builder
				printMessageSender(&b)
				fmt.Fprintf(&b, "support-state:\n")
				fmt.Fprintf(&b, "  %s", supportState.String())
				if err != nil {
					fmt.Fprintf(&b, "  %s\n", err.Error())
				}
				return b.String()

			case "set-time":
				var nowMilli int64
				d.ScanArgs(t, "now-milli", &nowMilli)
				manualTime.MustAdvanceTo(time.UnixMilli(nowMilli))
				testCallbackScheduler.tryRun()
				var b strings.Builder
				printMessageSender(&b)
				fmt.Fprintf(&b, "hlc: %s\n", clock.NowAsClockTimestamp())
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
