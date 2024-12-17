// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replica_rac2

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testReplica struct {
	raftNode *testRaftNode
	b        *strings.Builder

	leaseholder roachpb.ReplicaID
}

var _ ReplicaForTesting = &testReplica{}

func newTestReplica(b *strings.Builder) *testReplica {
	return &testReplica{b: b}
}

func (r *testReplica) initRaft(stable rac2.LogMark) {
	r.raftNode = &testRaftNode{
		b: r.b, r: r,
		term:              stable.Term,
		mark:              stable,
		nextUnstableIndex: stable.Index + 1,
	}
}

func (r *testReplica) IsScratchRange() bool {
	return true
}

type testRaftScheduler struct {
	b *strings.Builder
}

func (rs *testRaftScheduler) EnqueueRaftReady(id roachpb.RangeID) {
	fmt.Fprintf(rs.b, " RaftScheduler.EnqueueRaftReady(rangeID=%s)\n", id)
}

type testRaftNode struct {
	b *strings.Builder
	r *testReplica

	isLeader bool
	term     uint64
	leader   roachpb.ReplicaID

	mark              rac2.LogMark
	nextUnstableIndex uint64
}

func (rn *testRaftNode) SendPingRaftMuLocked(to roachpb.ReplicaID) bool {
	fmt.Fprintf(rn.b, " RaftNode.SendPingRaftMuLocked(%d)\n", to)
	return true
}

func (rn *testRaftNode) SendMsgAppRaftMuLocked(
	_ roachpb.ReplicaID, _ raft.LogSlice,
) (raftpb.Message, bool) {
	panic("unimplemented")
}

func (rn *testRaftNode) setMark(t *testing.T, mark rac2.LogMark) {
	require.True(t, mark.After(rn.mark))
	rn.mark = mark
}

func (rn *testRaftNode) check(t *testing.T) {
	if rn == nil {
		return
	}
	require.LessOrEqual(t, rn.mark.Term, rn.term)
	require.LessOrEqual(t, rn.nextUnstableIndex, rn.mark.Index+1)
}

func (rn *testRaftNode) print() {
	fmt.Fprintf(rn.b, "Raft: term: %d leader: %d leaseholder: %d mark: %+v next-unstable: %d",
		rn.term, rn.leader, rn.r.leaseholder, rn.mark, rn.nextUnstableIndex)
}

type testAdmittedPiggybacker struct {
	b *strings.Builder
}

func (p *testAdmittedPiggybacker) Add(
	n roachpb.NodeID, m kvflowcontrolpb.PiggybackedAdmittedState,
) {
	fmt.Fprintf(p.b, " Piggybacker.Add(n%d, %v)\n", n, m)
}

type testACWorkQueue struct {
	b *strings.Builder
	// TODO(sumeer): test case that sets this to true.
	returnFalse bool
}

func (q *testACWorkQueue) Admit(ctx context.Context, entry EntryForAdmission) bool {
	fmt.Fprintf(q.b, " ACWorkQueue.Admit(%+v) = %t\n", entry, !q.returnFalse)
	return !q.returnFalse
}

type testRangeControllerFactory struct {
	b   *strings.Builder
	rcs []*testRangeController
}

func (f *testRangeControllerFactory) New(
	ctx context.Context, state rangeControllerInitState,
) rac2.RangeController {
	fmt.Fprintf(f.b,
		" RangeControllerFactory.New(replicaSet=%s, leaseholder=%s, nextRaftIndex=%d, forceFlushIndex=%d)\n",
		state.replicaSet, state.leaseholder, state.nextRaftIndex, state.forceFlushIndex)
	rc := &testRangeController{b: f.b, waited: true}
	f.rcs = append(f.rcs, rc)
	return rc
}

type testRangeController struct {
	b              *strings.Builder
	waited         bool
	waitForEvalErr error
}

func (c *testRangeController) WaitForEval(
	ctx context.Context, pri admissionpb.WorkPriority,
) (bool, error) {
	errStr := "<nil>"
	if c.waitForEvalErr != nil {
		errStr = c.waitForEvalErr.Error()
	}
	fmt.Fprintf(c.b, " RangeController.WaitForEval(pri=%s) = (waited=%t err=%s)\n",
		pri.String(), c.waited, errStr)
	return c.waited, c.waitForEvalErr
}

type testMsgAppSender struct{}

func (testMsgAppSender) SendMsgApp(
	ctx context.Context, msg raftpb.Message, lowPriorityOverride bool,
) {
	// Do nothing, since only called by the real RangeController, which is not
	// used in this test.
}

func raftEventString(e rac2.RaftEvent) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[")
	for i := range e.Entries {
		prefix := " "
		if i == 0 {
			prefix = ""
		}
		fmt.Fprintf(&b, "%s%d", prefix, e.Entries[i].Index)
	}
	fmt.Fprintf(&b, "]")
	return b.String()
}

func (c *testRangeController) HandleRaftEventRaftMuLocked(
	ctx context.Context, e rac2.RaftEvent,
) error {
	fmt.Fprintf(c.b, " RangeController.HandleRaftEventRaftMuLocked(%s)\n", raftEventString(e))
	return nil
}

func (c *testRangeController) HandleSchedulerEventRaftMuLocked(
	_ context.Context, _ rac2.RaftMsgAppMode, _ raft.LogSnapshot,
) {
	panic("HandleSchedulerEventRaftMuLocked is unimplemented")
}

func (c *testRangeController) AdmitRaftMuLocked(
	_ context.Context, replicaID roachpb.ReplicaID, av rac2.AdmittedVector,
) {
	fmt.Fprintf(c.b, " RangeController.AdmitRaftMuLocked(%s, %+v)\n", replicaID, av)
}

func (c *testRangeController) MaybeSendPingsRaftMuLocked() {
	fmt.Fprintf(c.b, " RangeController.MaybeSendPingsRaftMuLocked()\n")
}

func (c *testRangeController) HoldsSendTokensLocked() bool {
	fmt.Fprintf(c.b, " RangeController.HoldsSendTokensLocked()\n")
	return false
}

func (c *testRangeController) SetReplicasRaftMuLocked(
	ctx context.Context, replicas rac2.ReplicaSet,
) error {
	fmt.Fprintf(c.b, " RangeController.SetReplicasRaftMuLocked(%s)\n", replicas)
	return nil
}

func (c *testRangeController) SetLeaseholderRaftMuLocked(
	ctx context.Context, replica roachpb.ReplicaID,
) {
	fmt.Fprintf(c.b, " RangeController.SetLeaseholderRaftMuLocked(%s)\n", replica)
}

func (c *testRangeController) ForceFlushIndexChangedLocked(ctx context.Context, index uint64) {
	fmt.Fprintf(c.b, " RangeController.ForceFlushIndexChangedLocked(%d)\n", index)
}

func (c *testRangeController) CloseRaftMuLocked(ctx context.Context) {
	fmt.Fprintf(c.b, " RangeController.CloseRaftMuLocked\n")
}

func (c *testRangeController) InspectRaftMuLocked(ctx context.Context) kvflowinspectpb.Handle {
	fmt.Fprintf(c.b, " RangeController.InspectRaftMuLocked\n")
	return kvflowinspectpb.Handle{}
}

func (c *testRangeController) SendStreamStats(stats *rac2.RangeSendStreamStats) {
	fmt.Fprintf(c.b, " RangeController.SendStreamStats\n")
}

func (c *testRangeController) StatusRaftMuLocked() serverpb.RACStatus {
	fmt.Fprintf(c.b, " RangeController.StatusRaftMuLocked\n")
	return serverpb.RACStatus{}
}

func makeTestMutexAsserter() rac2.ReplicaMutexAsserter {
	var raftMu syncutil.Mutex
	var replicaMu syncutil.RWMutex
	return rac2.MakeReplicaMutexAsserter(&raftMu, &replicaMu)
}

func LockRaftMuAndReplicaMu(mu *rac2.ReplicaMutexAsserter) (unlockFunc func()) {
	mu.RaftMu.Lock()
	mu.ReplicaMu.Lock()
	return func() {
		mu.ReplicaMu.Unlock()
		mu.RaftMu.Unlock()
	}
}

func LockRaftMu(mu *rac2.ReplicaMutexAsserter) (unlockFunc func()) {
	mu.RaftMu.Lock()
	return func() {
		mu.RaftMu.Unlock()
	}
}

func TestProcessorBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const replicaID = 5

	ctx := context.Background()
	var b strings.Builder
	var r *testReplica
	var sched testRaftScheduler
	var piggybacker testAdmittedPiggybacker
	var q testACWorkQueue
	var rcFactory testRangeControllerFactory
	var p *processorImpl
	tenantID := roachpb.MustMakeTenantID(4)
	muAsserter := makeTestMutexAsserter()
	reset := func(enabled kvflowcontrol.V2EnabledWhenLeaderLevel) {
		b.Reset()
		r = newTestReplica(&b)
		sched = testRaftScheduler{b: &b}
		piggybacker = testAdmittedPiggybacker{b: &b}
		q = testACWorkQueue{b: &b}
		rcFactory = testRangeControllerFactory{b: &b}
		p = NewProcessor(ProcessorOptions{
			NodeID:                 1,
			StoreID:                2,
			RangeID:                3,
			ReplicaID:              replicaID,
			ReplicaForTesting:      r,
			ReplicaMutexAsserter:   muAsserter,
			RaftScheduler:          &sched,
			AdmittedPiggybacker:    &piggybacker,
			ACWorkQueue:            &q,
			MsgAppSender:           testMsgAppSender{},
			RangeControllerFactory: &rcFactory,
			EnabledWhenLeaderLevel: enabled,
			EvalWaitMetrics:        rac2.NewEvalWaitMetrics(),
		}).(*processorImpl)
		fmt.Fprintf(&b, "n%s,s%s,r%s: replica=%s, tenant=%s, enabled-level=%s\n",
			p.opts.NodeID, p.opts.StoreID, p.opts.RangeID, p.opts.ReplicaID, tenantID,
			enabledLevelString(p.GetEnabledWhenLeader()))
	}
	builderStr := func() string {
		str := b.String()
		b.Reset()
		return str
	}
	printLogTracker := func() {
		fmt.Fprint(&b, p.logTracker.debugString())
	}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "processor"),
		func(t *testing.T, d *datadriven.TestData) string {
			defer func() { r.raftNode.check(t) }()

			switch d.Cmd {
			case "reset":
				enabledLevel := parseEnabledLevel(t, d)
				reset(enabledLevel)
				return builderStr()

			case "init-raft":
				var mark rac2.LogMark
				d.ScanArgs(t, "log-term", &mark.Term)
				d.ScanArgs(t, "log-index", &mark.Index)
				r.initRaft(mark)
				unlockFunc := LockRaftMuAndReplicaMu(&muAsserter)
				p.InitRaftLocked(ctx, r.raftNode, r.raftNode.mark)
				unlockFunc()
				return builderStr()

			case "set-raft-state":
				if d.HasArg("leader") {
					var leaderID int
					d.ScanArgs(t, "leader", &leaderID)
					r.raftNode.isLeader = leaderID == replicaID
					r.raftNode.leader = roachpb.ReplicaID(leaderID)
				}
				if d.HasArg("next-unstable-index") {
					var nextUnstableIndex uint64
					d.ScanArgs(t, "next-unstable-index", &nextUnstableIndex)
					r.raftNode.nextUnstableIndex = nextUnstableIndex
				}
				if d.HasArg("term") {
					var term uint64
					d.ScanArgs(t, "term", &term)
					r.raftNode.term = term
				}
				if d.HasArg("leaseholder") {
					var leaseholder int
					d.ScanArgs(t, "leaseholder", &leaseholder)
					r.leaseholder = roachpb.ReplicaID(leaseholder)
				}
				if d.HasArg("log-term") {
					var mark rac2.LogMark
					d.ScanArgs(t, "log-term", &mark.Term)
					d.ScanArgs(t, "log-index", &mark.Index)
					r.raftNode.setMark(t, mark)
				}
				r.raftNode.print()
				return builderStr()

			case "synced-log":
				var mark rac2.LogMark
				d.ScanArgs(t, "term", &mark.Term)
				d.ScanArgs(t, "index", &mark.Index)
				p.SyncedLogStorage(ctx, mark)
				printLogTracker()
				return builderStr()

			case "on-destroy":
				unlockFunc := LockRaftMu(&muAsserter)
				p.OnDestroyRaftMuLocked(ctx)
				unlockFunc()
				return builderStr()

			case "set-enabled-level":
				enabledLevel := parseEnabledLevel(t, d)
				var state RaftNodeBasicState
				if r.raftNode != nil {
					state = RaftNodeBasicState{
						Term:              r.raftNode.term,
						IsLeader:          r.raftNode.isLeader,
						Leader:            r.raftNode.leader,
						NextUnstableIndex: r.raftNode.nextUnstableIndex,
						Leaseholder:       r.leaseholder,
					}
				}
				unlockFunc := LockRaftMu(&muAsserter)
				p.SetEnabledWhenLeaderRaftMuLocked(ctx, enabledLevel, state)
				unlockFunc()
				return builderStr()

			case "get-enabled-level":
				enabledLevel := p.GetEnabledWhenLeader()
				fmt.Fprintf(&b, "enabled-level: %s\n", enabledLevelString(enabledLevel))
				return builderStr()

			case "on-desc-changed":
				desc := parseRangeDescriptor(t, d)
				unlockFunc := LockRaftMuAndReplicaMu(&muAsserter)
				p.OnDescChangedLocked(ctx, &desc, tenantID)
				unlockFunc()
				return builderStr()

			case "handle-raft-ready-and-admit":
				// We don't bother setting RaftEvent.ReplicasStateInfo since it is
				// unused by processorImpl, and simply passed down to RangeController
				// (which we've mocked out in this test).
				var event rac2.RaftEvent
				if d.HasArg("entries") {
					var arg string
					d.ScanArgs(t, "entries", &arg)
					event.Entries = createEntries(t, parseEntryInfos(t, arg))
				}
				if len(event.Entries) > 0 {
					d.ScanArgs(t, "leader-term", &event.Term)
				}
				fmt.Fprintf(&b, "HandleRaftReady:\n")
				var state RaftNodeBasicState
				if r.raftNode != nil {
					state = RaftNodeBasicState{
						Term:              r.raftNode.term,
						IsLeader:          r.raftNode.isLeader,
						Leader:            r.raftNode.leader,
						NextUnstableIndex: r.raftNode.nextUnstableIndex,
						Leaseholder:       r.leaseholder,
					}
				}
				unlockFunc := LockRaftMu(&muAsserter)
				p.HandleRaftReadyRaftMuLocked(ctx, state, event)
				fmt.Fprintf(&b, ".....\n")
				if len(event.Entries) > 0 {
					fmt.Fprintf(&b, "AdmitRaftEntries:\n")
					destroyedOrV2 := p.AdmitRaftEntriesRaftMuLocked(ctx, event)
					fmt.Fprintf(&b, "destroyed-or-leader-using-v2: %t\n", destroyedOrV2)
					printLogTracker()
				}
				unlockFunc()
				return builderStr()

			case "enqueue-piggybacked-admitted":
				var from, to uint64
				d.ScanArgs(t, "from", &from)
				d.ScanArgs(t, "to", &to)
				require.Equal(t, p.opts.ReplicaID, roachpb.ReplicaID(to))

				var term, index, pri int
				d.ScanArgs(t, "term", &term)
				d.ScanArgs(t, "index", &index)
				d.ScanArgs(t, "pri", &pri)
				require.Less(t, pri, int(raftpb.NumPriorities))
				as := kvflowcontrolpb.AdmittedState{
					Term:     uint64(term),
					Admitted: make([]uint64, raftpb.NumPriorities),
				}
				as.Admitted[pri] = uint64(index)

				p.EnqueuePiggybackedAdmittedAtLeader(roachpb.ReplicaID(from), as)
				return builderStr()

			case "process-piggybacked-admitted":
				unlockFunc := LockRaftMu(&muAsserter)
				p.ProcessPiggybackedAdmittedAtLeaderRaftMuLocked(ctx)
				unlockFunc()
				return builderStr()

			case "side-channel":
				var usingV2 bool
				if d.HasArg("v2") {
					usingV2 = true
				}
				var leaderTerm uint64
				d.ScanArgs(t, "leader-term", &leaderTerm)
				var first, last uint64
				d.ScanArgs(t, "first", &first)
				d.ScanArgs(t, "last", &last)
				var lowPriOverride bool
				if d.HasArg("low-pri") {
					lowPriOverride = true
				}
				info := SideChannelInfoUsingRaftMessageRequest{
					UsingV2Protocol: usingV2,
					LeaderTerm:      leaderTerm,
					First:           first,
					Last:            last,
					LowPriOverride:  lowPriOverride,
				}
				unlockFunc := LockRaftMu(&muAsserter)
				p.SideChannelForPriorityOverrideAtFollowerRaftMuLocked(info)
				unlockFunc()
				return builderStr()

			case "admitted-log-entry":
				var cb EntryForAdmissionCallbackState
				d.ScanArgs(t, "leader-term", &cb.Mark.Term)
				d.ScanArgs(t, "index", &cb.Mark.Index)
				var pri int
				d.ScanArgs(t, "pri", &pri)
				cb.Priority = raftpb.Priority(pri)
				p.AdmittedLogEntry(ctx, cb)
				printLogTracker()
				return builderStr()

			case "admit-for-eval":
				pri := parseAdmissionPriority(t, d)
				// The callee ignores the create time.
				admitted, err := p.AdmitForEval(ctx, pri, time.Time{})
				fmt.Fprintf(&b, "admitted: %t err: ", admitted)
				if err == nil {
					fmt.Fprintf(&b, "<nil>\n")
				} else {
					fmt.Fprintf(&b, "%s\n", err.Error())
				}
				return builderStr()

			case "set-wait-for-eval-return-values":
				rc := rcFactory.rcs[len(rcFactory.rcs)-1]
				d.ScanArgs(t, "waited", &rc.waited)
				rc.waitForEvalErr = nil
				if d.HasArg("err") {
					var errStr string
					d.ScanArgs(t, "err", &errStr)
					rc.waitForEvalErr = errors.Errorf("%s", errStr)
				}
				return builderStr()

			case "inspect":
				unlockFunc := LockRaftMu(&muAsserter)
				p.InspectRaftMuLocked(ctx)
				unlockFunc()
				return builderStr()

			case "send-stream-stats":
				stats := rac2.RangeSendStreamStats{}
				p.SendStreamStats(&stats)
				return builderStr()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func parseAdmissionPriority(t *testing.T, td *datadriven.TestData) admissionpb.WorkPriority {
	var priStr string
	td.ScanArgs(t, "pri", &priStr)
	for k, v := range admissionpb.WorkPriorityDict {
		if v == priStr {
			return k
		}
	}
	t.Fatalf("unknown priority %s", priStr)
	return admissionpb.NormalPri
}

func parseEnabledLevel(
	t *testing.T, td *datadriven.TestData,
) kvflowcontrol.V2EnabledWhenLeaderLevel {
	if td.HasArg("enabled-level") {
		var str string
		td.ScanArgs(t, "enabled-level", &str)
		switch str {
		case "not-enabled":
			return kvflowcontrol.V2NotEnabledWhenLeader
		case "v1-encoding":
			return kvflowcontrol.V2EnabledWhenLeaderV1Encoding
		case "v2-encoding":
			return kvflowcontrol.V2EnabledWhenLeaderV2Encoding
		default:
			t.Fatalf("unrecoginized level %s", str)
		}
	}
	return kvflowcontrol.V2NotEnabledWhenLeader
}

func enabledLevelString(enabledLevel kvflowcontrol.V2EnabledWhenLeaderLevel) string {
	switch enabledLevel {
	case kvflowcontrol.V2NotEnabledWhenLeader:
		return "not-enabled"
	case kvflowcontrol.V2EnabledWhenLeaderV1Encoding:
		return "v1-encoding"
	case kvflowcontrol.V2EnabledWhenLeaderV2Encoding:
		return "v2-encoding"
	}
	return "unknown-level"
}

func parseRangeDescriptor(t *testing.T, td *datadriven.TestData) roachpb.RangeDescriptor {
	var replicaStr string
	td.ScanArgs(t, "replicas", &replicaStr)
	parts := strings.Split(replicaStr, ",")
	var desc roachpb.RangeDescriptor
	for _, part := range parts {
		replica := parseReplicaDescriptor(t, strings.TrimSpace(part))
		desc.InternalReplicas = append(desc.InternalReplicas, replica)
	}
	return desc
}

// n<int>/s<int>/<int>{/<type>}
// Where type is {voter_full, non_voter}.
func parseReplicaDescriptor(t *testing.T, arg string) roachpb.ReplicaDescriptor {
	parts := strings.Split(arg, "/")
	require.LessOrEqual(t, 3, len(parts))
	require.GreaterOrEqual(t, 4, len(parts))
	ni, err := strconv.Atoi(strings.TrimPrefix(parts[0], "n"))
	require.NoError(t, err)
	store, err := strconv.Atoi(strings.TrimPrefix(parts[1], "s"))
	require.NoError(t, err)
	repl, err := strconv.Atoi(parts[2])
	require.NoError(t, err)
	typ := roachpb.VOTER_FULL
	if len(parts) == 4 {
		switch parts[3] {
		case "voter_full":
		case "non_voter":
			typ = roachpb.NON_VOTER
		default:
			t.Fatalf("unknown replica type %s", parts[3])
		}
	}
	var desc roachpb.ReplicaDescriptor
	desc.NodeID = roachpb.NodeID(ni)
	desc.StoreID = roachpb.StoreID(store)
	desc.ReplicaID = roachpb.ReplicaID(repl)
	desc.Type = typ
	return desc
}

type entryInfo struct {
	encoding   raftlog.EntryEncoding
	index      uint64
	term       uint64
	pri        raftpb.Priority
	createTime int64
	length     int
}

func createEntry(t *testing.T, info entryInfo) raftpb.Entry {
	cmdID := kvserverbase.CmdIDKey("11111111")
	var metaBuf []byte
	if info.encoding.UsesAdmissionControl() {
		meta := kvflowcontrolpb.RaftAdmissionMeta{
			AdmissionCreateTime: info.createTime,
		}
		isV2Encoding := info.encoding == raftlog.EntryEncodingStandardWithACAndPriority ||
			info.encoding == raftlog.EntryEncodingSideloadedWithACAndPriority
		if isV2Encoding {
			meta.AdmissionPriority = int32(info.pri)
		} else {
			meta.AdmissionOriginNode = 10
			require.Equal(t, raftpb.LowPri, info.pri)
			meta.AdmissionPriority = int32(raftpb.LowPri)
		}
		var err error
		metaBuf, err = protoutil.Marshal(&meta)
		require.NoError(t, err)
	}
	cmdBufPrefix := raftlog.EncodeCommandBytes(info.encoding, cmdID, nil, info.pri)
	paddingLen := info.length - len(cmdBufPrefix) - len(metaBuf)
	// Padding also needs to decode as part of the RaftCommand proto, so we
	// abuse the WriteBatch.Data field which is a byte slice. Since it is a
	// nested field it consumes two tags plus two lengths. We'll approximate
	// this as needing a maximum of 15 bytes, to be on the safe side.
	require.LessOrEqual(t, 15, paddingLen)
	cmd := kvserverpb.RaftCommand{
		WriteBatch: &kvserverpb.WriteBatch{Data: make([]byte, paddingLen)}}
	// Shrink by 1 on each iteration. This doesn't give us a guarantee that we
	// will get exactly paddingLen since the length of data affects the encoded
	// lengths, but it should usually work, and cause fewer questions when
	// looking at the testdata file.
	for cmd.Size() > paddingLen {
		cmd.WriteBatch.Data = cmd.WriteBatch.Data[:len(cmd.WriteBatch.Data)-1]
	}
	cmdBuf, err := protoutil.Marshal(&cmd)
	require.NoError(t, err)
	data := append(cmdBufPrefix, metaBuf...)
	data = append(data, cmdBuf...)
	return raftpb.Entry{
		Term:  info.term,
		Index: info.index,
		Type:  raftpb.EntryNormal,
		Data:  data,
	}
}

func createEntries(t *testing.T, infos []entryInfo) []raftpb.Entry {
	var entries []raftpb.Entry
	for _, info := range infos {
		entries = append(entries, createEntry(t, info))
	}
	return entries
}

// encoding, index, priority, create-time, length.
// <enc>/i<int>/t<int>/pri<int>/time<int>/len<int>
func parseEntryInfos(t *testing.T, arg string) []entryInfo {
	parts := strings.Split(arg, ",")
	var entries []entryInfo
	for _, part := range parts {
		entries = append(entries, parseEntryInfo(t, strings.TrimSpace(part)))
	}
	return entries
}

func parseEntryInfo(t *testing.T, arg string) entryInfo {
	parts := strings.Split(arg, "/")
	require.Equal(t, 6, len(parts))
	encoding := parseEntryEncoding(t, strings.TrimSpace(parts[0]))
	index, err := strconv.Atoi(strings.TrimPrefix(parts[1], "i"))
	require.NoError(t, err)
	term, err := strconv.Atoi(strings.TrimPrefix(parts[2], "t"))
	require.NoError(t, err)
	pri, err := strconv.Atoi(strings.TrimPrefix(parts[3], "pri"))
	require.NoError(t, err)
	createTime, err := strconv.Atoi(strings.TrimPrefix(parts[4], "time"))
	require.NoError(t, err)
	length, err := strconv.Atoi(strings.TrimPrefix(parts[5], "len"))
	require.NoError(t, err)
	return entryInfo{
		encoding:   encoding,
		index:      uint64(index),
		term:       uint64(term),
		pri:        raftpb.Priority(pri),
		createTime: int64(createTime),
		length:     length,
	}
}

func parseEntryEncoding(t *testing.T, arg string) raftlog.EntryEncoding {
	switch arg {
	case "v1":
		return raftlog.EntryEncodingStandardWithAC
	case "v2":
		return raftlog.EntryEncodingStandardWithACAndPriority
	case "v1-side":
		return raftlog.EntryEncodingSideloadedWithAC
	case "v2-side":
		return raftlog.EntryEncodingSideloadedWithACAndPriority
	case "none":
		return raftlog.EntryEncodingStandardWithoutAC
	default:
		t.Fatalf("unrecognized encoding string %s", arg)
	}
	return raftlog.EntryEncodingEmpty
}
