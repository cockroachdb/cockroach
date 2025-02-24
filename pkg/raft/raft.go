// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/raft/confchange"
	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"golang.org/x/exp/maps"
)

const (
	// None is a placeholder node ID used when there is no leader.
	//
	// TODO(arul): consider pulling these into raftpb as well.
	None pb.PeerID = 0
	// LocalAppendThread is a reference to a local thread that saves unstable
	// log entries and snapshots to stable storage. The identifier is used as a
	// target for MsgStorageAppend messages when AsyncStorageWrites is enabled.
	LocalAppendThread pb.PeerID = math.MaxUint64
	// LocalApplyThread is a reference to a local thread that applies committed
	// log entries to the local state machine. The identifier is used as a
	// target for MsgStorageApply messages when AsyncStorageWrites is enabled.
	LocalApplyThread pb.PeerID = math.MaxUint64 - 1
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

const noLimit = math.MaxUint64

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Int63n).
type lockedRand struct {
	mu sync.Mutex
}

func (r *lockedRand) Int63n(n int64) int64 {
	r.mu.Lock()
	v, _ := rand.Int(rand.Reader, big.NewInt(n))
	r.mu.Unlock()
	return v.Int64()
}

var globalRand = &lockedRand{}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID pb.PeerID

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int64
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int64

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64

	// AsyncStorageWrites configures the raft node to write to its local storage
	// (raft log and state machine) using a request/response message passing
	// interface instead of the default Ready/Advance function call interface.
	// Local storage messages can be pipelined and processed asynchronously
	// (with respect to Ready iteration), facilitating reduced interference
	// between Raft proposals and increased batching of log appends and state
	// machine application. As a result, use of asynchronous storage writes can
	// reduce end-to-end commit latency and increase maximum throughput.
	//
	// When true, the Ready.Message slice will include MsgStorageAppend and
	// MsgStorageApply messages. The messages will target a LocalAppendThread
	// and a LocalApplyThread, respectively. Messages to the same target must be
	// reliably processed in order. In other words, they can't be dropped (like
	// messages over the network) and those targeted at the same thread can't be
	// reordered. Messages to different targets can be processed in any order.
	//
	// MsgStorageAppend carries Raft log entries to append, election votes /
	// term changes / updated commit indexes to persist, and snapshots to apply.
	// All writes performed in service of a MsgStorageAppend must be durable
	// before response messages are delivered. However, if the MsgStorageAppend
	// carries no response messages, durability is not required. The message
	// assumes the role of the Entries, HardState, and Snapshot fields in Ready.
	//
	// MsgStorageApply carries committed entries to apply. Writes performed in
	// service of a MsgStorageApply need not be durable before response messages
	// are delivered. The message assumes the role of the CommittedEntries field
	// in Ready.
	//
	// Local messages each carry one or more response messages which should be
	// delivered after the corresponding storage write has been completed. These
	// responses may target the same node or may target other nodes. The storage
	// threads are not responsible for understanding the response messages, only
	// for delivering them to the correct target after performing the storage
	// write.
	// TODO(#129411): deprecate !AsyncStorageWrites mode as it's not used in
	// CRDB.
	AsyncStorageWrites bool
	// LazyReplication instructs raft to hold off constructing MsgApp messages
	// eagerly in reaction to Step() calls.
	//
	// If LazyReplication is false, a MsgApp can be constructed any time it
	// becomes possible, e.g. when a new entry is appended to the leader's log, or
	// the in-flight volume to a peer drops below the max-inflight limits.
	//
	// If LazyReplication is true, MsgApp messages for StateReplicate peers are
	// constructed on demand, when requested by the application layer via the
	// RawNode.SendMsgApp method.
	LazyReplication bool

	// MaxSizePerMsg limits the max byte size of each append message. Smaller
	// value lowers the raft recovery cost(initial probing and message lost
	// during normal operation). On the other side, it might affect the
	// throughput during normal replication. Note: math.MaxUint64 for unlimited,
	// 0 for at most one entry per message.
	MaxSizePerMsg uint64
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applying at the same time.
	//
	// Despite its name (preserved for compatibility), this quota applies across
	// Ready structs to encompass all outstanding entries in unacknowledged
	// MsgStorageApply messages when AsyncStorageWrites is enabled.
	MaxCommittedSizePerReady uint64
	// MaxUncommittedEntriesSize limits the aggregate byte size of the
	// uncommitted entries that may be appended to a leader's log. Once this
	// limit is exceeded, proposals will begin to return ErrProposalDropped
	// errors. Note: 0 for no limit.
	MaxUncommittedEntriesSize uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	MaxInflightMsgs int
	// MaxInflightBytes limits the number of in-flight bytes in append messages.
	// Complements MaxInflightMsgs. Ignored if zero.
	//
	// This effectively bounds the bandwidth-delay product. Note that especially
	// in high-latency deployments setting this too low can lead to a dramatic
	// reduction in throughput. For example, with a peer that has a round-trip
	// latency of 100ms to the leader and this setting is set to 1 MB, there is a
	// throughput limit of 10 MB/s for this group. With RTT of 400ms, this drops
	// to 2.5 MB/s. See Little's law to understand the maths behind.
	MaxInflightBytes uint64

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger raftlogger.Logger

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an inaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool

	// DisableConfChangeValidation turns off propose-time verification of
	// configuration changes against the currently active configuration of the
	// raft instance. These checks are generally sensible (cannot leave a joint
	// config unless in a joint config, et cetera) but they have false positives
	// because the active configuration may not be the most recent
	// configuration. This is because configurations are activated during log
	// application, and even the leader can trail log application by an
	// unbounded number of entries.
	// Symmetrically, the mechanism has false negatives - because the check may
	// not run against the "actual" config that will be the predecessor of the
	// newly proposed one, the check may pass but the new config may be invalid
	// when it is being applied. In other words, the checks are best-effort.
	//
	// Users should *not* use this option unless they have a reliable mechanism
	// (above raft) that serializes and verifies configuration changes. If an
	// invalid configuration change enters the log and gets applied, a panic
	// will result.
	//
	// This option may be removed once false positives are no longer possible.
	// See: https://github.com/etcd-io/raft/issues/80
	DisableConfChangeValidation bool

	// StoreLiveness is a reference to the store liveness fabric.
	StoreLiveness raftstoreliveness.StoreLiveness

	// CRDBVersion exposes the active version to Raft. This helps version-gating
	// features.
	CRDBVersion clusterversion.Handle

	Metrics      *Metrics
	TestingKnobs *TestingKnobs
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}
	if IsLocalMsgTarget(c.ID) {
		return errors.New("cannot use local target as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}

	// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
	// previously the same parameter.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}
	if c.MaxInflightBytes == 0 {
		c.MaxInflightBytes = noLimit
	} else if c.MaxInflightBytes < c.MaxSizePerMsg {
		return errors.New("max inflight bytes must be >= max message size")
	}

	if c.Logger == nil {
		c.Logger = raftlogger.GetLogger()
	}

	return nil
}

type raft struct {
	id pb.PeerID

	Term uint64
	Vote pb.PeerID

	// the log
	raftLog *raftLog

	maxMsgSize         entryEncodingSize
	maxUncommittedSize entryPayloadSize

	config               quorum.Config
	trk                  tracker.ProgressTracker
	electionTracker      tracker.ElectionTracker
	fortificationTracker *tracker.FortificationTracker
	lazyReplication      bool

	state pb.StateType

	// idxPreLeading is the last log index as of when this node became the
	// leader. Separates entries proposed by previous leaders from the entries
	// proposed by the current leader. Used only in StateLeader, and updated
	// when entering StateLeader (in becomeLeader()).
	//
	// Invariants (when in StateLeader at raft.Term):
	//	- entries at indices <= idxPreLeading have term < raft.Term
	//	- entries at indices > idxPreLeading have term == raft.Term
	idxPreLeading uint64

	// isLearner is true if the local raft node is a learner.
	isLearner bool

	// msgs contains the list of messages that should be sent out immediately to
	// other nodes.
	//
	// Messages in this list must target other nodes.
	msgs []pb.Message
	// msgsAfterAppend contains the list of messages that should be sent after
	// the accumulated unstable state (e.g. term, vote, []entry, and snapshot)
	// has been persisted to durable storage. This includes waiting for any
	// unstable state that is already in the process of being persisted (i.e.
	// has already been handed out in a prior Ready struct) to complete.
	//
	// Messages in this list may target other nodes or may target this node.
	//
	// Messages in this list have the type MsgAppResp, MsgVoteResp,
	// MsgPreVoteResp, or MsgFortifyLeaderResp. See the comment in raft.send for
	// details.
	msgsAfterAppend []pb.Message

	// the leader id
	lead pb.PeerID
	// leadEpoch, if set, corresponds to the StoreLiveness epoch that this peer
	// has supported the leader in. It's unset if the peer hasn't supported the
	// current leader.
	leadEpoch pb.Epoch
	// leadTransferee, if set, is the id of the leader transfer target during a
	// pending leadership transfer. The value is set while the outgoing leader
	// (this node) is catching the target up on its log. During this time, the
	// leader will drop incoming proposals to give the transfer target time to
	// catch up. Once the transfer target is caught up, the leader will send it
	// a MsgTimeoutNow to encourage it to campaign immediately while bypassing
	// pre-vote and leader support safeguards. As soon as the MsgTimeoutNow is
	// sent, the leader will step down to a follower, as it has irrevocably
	// compromised its leadership term by giving the target permission to
	// overthrow it.
	//
	// For cases where the transfer target is already caught up on the log at the
	// time that the leader receives a MsgTransferLeader, the MsgTimeoutNow will
	// be sent immediately and the leader will step down to a follower without
	// ever setting this field.
	//
	// In either case, if the transfer fails after the MsgTimeoutNow has been
	// sent, the leader (who has stepped down to a follower) must call a new
	// election at a new term in order to reestablish leadership.
	//
	// This roughly follows the procedure defined in the raft thesis, section
	// 3.10: Leadership transfer extension.
	leadTransferee pb.PeerID
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	pendingConfIndex uint64
	// disableConfChangeValidation is Config.DisableConfChangeValidation,
	// see there for details.
	disableConfChangeValidation bool
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	uncommittedSize entryPayloadSize

	// electionElapsed is tracked by both leaders and followers. For followers, it
	// is the number of ticks since they last received a valid message from the
	// from the current leader, unless the follower is fortifying a leader
	// (leadEpoch != 0), in which case it is always set to 0. For leaders, it is
	// the number of ticks since the last time it performed a checkQuorum.
	//
	// Invariant: electionElapsed = 0 when r.leadEpoch != 0 on a follower.
	electionElapsed int64

	// heartbeatElapsed is the number of ticks since we last reached the
	// heartbeatTimeout. Leaders use this field to keep track of when they should
	// broadcast fortification attempts, and in a pre-fortification world,
	// heartbeats. Followers use this field to keep track of when they should
	// broadcast de-fortification messages to peers.
	//
	// TODO(arul): consider renaming these to "fortifyElapsed" given heartbeats
	// are no longer the first class concept they used to be pre-leader
	// fortification.
	heartbeatElapsed int64

	maxInflight      int
	maxInflightBytes uint64
	checkQuorum      bool
	preVote          bool

	heartbeatTimeout int64
	electionTimeout  int64
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int64
	disableProposalForwarding bool

	tick func()
	step stepFunc

	logger        raftlogger.Logger
	storeLiveness raftstoreliveness.StoreLiveness
	crdbVersion   clusterversion.Handle
	metrics       *Metrics
	testingKnobs  *TestingKnobs
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLogWithSize(c.Storage, c.Logger, entryEncodingSize(c.MaxCommittedSizePerReady))
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}

	r := &raft{
		id:                          c.ID,
		isLearner:                   false,
		raftLog:                     raftlog,
		maxMsgSize:                  entryEncodingSize(c.MaxSizePerMsg),
		maxUncommittedSize:          entryPayloadSize(c.MaxUncommittedEntriesSize),
		lazyReplication:             c.LazyReplication,
		electionTimeout:             c.ElectionTick,
		heartbeatTimeout:            c.HeartbeatTick,
		logger:                      c.Logger,
		maxInflight:                 c.MaxInflightMsgs,
		maxInflightBytes:            c.MaxInflightBytes,
		checkQuorum:                 c.CheckQuorum,
		preVote:                     c.PreVote,
		disableProposalForwarding:   c.DisableProposalForwarding,
		disableConfChangeValidation: c.DisableConfChangeValidation,
		storeLiveness:               c.StoreLiveness,
		crdbVersion:                 c.CRDBVersion,
		metrics:                     c.Metrics,
		testingKnobs:                c.TestingKnobs,
	}
	lastID := r.raftLog.lastEntryID()

	r.electionTracker = tracker.MakeElectionTracker(&r.config)
	r.fortificationTracker = tracker.NewFortificationTracker(&r.config, r.storeLiveness, r.logger)

	cfg, progressMap, err := confchange.Restore(confchange.Changer{
		Config:           quorum.MakeEmptyConfig(),
		ProgressMap:      tracker.MakeEmptyProgressMap(),
		MaxInflight:      r.maxInflight,
		MaxInflightBytes: r.maxInflightBytes,
		LastIndex:        lastID.index,
	}, cs)
	if err != nil {
		panic(err)
	}
	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, progressMap))

	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied, 0 /* size */)
	}

	if r.lead == r.id {
		// If we were the leader, we must have waited out the leadMaxSupported. This
		// is done in the kvserver layer before reaching this point. Therefore, it
		// should be safe to defortify and become a follower while forgetting that
		// we were the leader. If we don't forget that we were the leader, it will
		// lead to situations where r.id == r.lead but r.state != StateLeader which
		// might confuse the layers above raft.
		r.deFortify(r.id, r.Term)
		r.becomeFollower(r.Term, None)
	} else {
		// If we weren't the leader, we should NOT forget who the leader is to avoid
		// regressing the leadMaxSupported. We can't just forget the leader because
		// we might have been fortifying a leader before the restart and need to
		// uphold our fortification promise after the restart as well. To do so, we
		// need to remember the leader and the fortified epoch, otherwise we may
		// vote for a different candidate or prematurely call an election, either of
		// which could regress the LeadSupportUntil.
		r.becomeFollower(r.Term, r.lead)
	}

	var nodesStrs []string
	for _, n := range r.trk.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	// TODO(pav-kv): it should be ok to simply print %+v for lastID.
	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, lastID.index, lastID.term)
	return r
}

func (r *raft) softState() SoftState { return SoftState{RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:      r.Term,
		Vote:      r.Vote,
		Commit:    r.raftLog.committed,
		Lead:      r.lead,
		LeadEpoch: r.leadEpoch,
	}
}

// send prepares the given message for being sent, and puts it into the outgoing
// message queue. The message will be handed over to the application via the
// next Ready handling cycle, except in one condition below.
//
// Certain message types are scheduled for being sent *after* the unstable state
// is durably persisted in storage. If AsyncStorageWrites config flag is true,
// the responsibility of upholding this condition is on the application, so the
// message will be handed over via the next Ready as usually; if false, the
// message will skip one Ready handling cycle, and will be sent after the
// application has persisted the state.
//
// TODO(pav-kv): remove this special case after !AsyncStorageWrites is removed.
func (r *raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	switch m.Type {
	case pb.MsgVote, pb.MsgVoteResp, pb.MsgPreVote, pb.MsgPreVoteResp, pb.MsgDeFortifyLeader:
		if m.Term == 0 {
			// All {pre-,}campaign messages and MsgDeFortifyLeader need to have the
			// term set when sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			// - MsgDeFortifyLeader: m.Term is the term corresponding to the
			// leadership term that's being de-fortified.
			r.logger.Panicf("term should be set when sending %s", m.Type)
		}
	case pb.MsgApp:
		if m.Term != r.Term {
			r.logger.Panicf("invalid term %d in MsgApp, must be %d", m.Term, r.Term)
		}
	default:
		if m.Term != 0 {
			r.logger.Panicf("term should not be set when sending %s (was %d)", m.Type, m.Term)
		}
		// Do not attach term to MsgProp. Proposals are a way to forward to the
		// leader, and should be treated as local message.
		if m.Type != pb.MsgProp {
			m.Term = r.Term
		}
	}

	switch m.Type {
	case pb.MsgAppResp, pb.MsgVoteResp, pb.MsgPreVoteResp, pb.MsgFortifyLeaderResp:
		// If async storage writes are enabled, messages added to the msgs slice
		// are allowed to be sent out before unstable state (e.g. log entry
		// writes and election votes) have been durably synced to the local
		// disk.
		//
		// For most message types, this is not an issue. However, response messages
		// that relate to "voting" on either leader election or log appends or
		// leader fortification require durability before they can be sent. It would
		// be incorrect to publish a vote in an election before that vote has been
		// synced to stable storage locally. Similarly, it would be incorrect to
		// acknowledge a log append to the leader before that entry has been synced
		// to stable storage locally. Similarly, it would also be incorrect to
		// promise fortification to a leader without durably persisting the
		// leader's epoch being supported.
		//
		// Per the Raft thesis, section 3.8 Persisted state and server restarts:
		//
		// > Raft servers must persist enough information to stable storage to
		// > survive server restarts safely. In particular, each server persists
		// > its current term and vote; this is necessary to prevent the server
		// > from voting twice in the same term or replacing log entries from a
		// > newer leader with those from a deposed leader. Each server also
		// > persists new log entries before they are counted towards the entries’
		// > commitment; this prevents committed entries from being lost or
		// > “uncommitted” when servers restart
		//
		// To enforce this durability requirement, these response messages are
		// queued to be sent out as soon as the current collection of unstable
		// state (the state that the response message was predicated upon) has
		// been durably persisted. This unstable state may have already been
		// passed to a Ready struct whose persistence is in progress or may be
		// waiting for the next Ready struct to begin being written to Storage.
		// These messages must wait for all of this state to be durable before
		// being published.
		//
		// Rejected responses (m.Reject == true) present an interesting case
		// where the durability requirement is less unambiguous. A rejection may
		// be predicated upon unstable state. For instance, a node may reject a
		// vote for one peer because it has already begun syncing its vote for
		// another peer. Or it may reject a vote from one peer because it has
		// unstable log entries that indicate that the peer is behind on its
		// log. In these cases, it is likely safe to send out the rejection
		// response immediately without compromising safety in the presence of a
		// server restart. However, because these rejections are rare and
		// because the safety of such behavior has not been formally verified,
		// we err on the side of safety and omit a `&& !m.Reject` condition
		// above.
		r.msgsAfterAppend = append(r.msgsAfterAppend, m)
	default:
		if m.To == r.id {
			r.logger.Panicf("message should not be self-addressed when sending %s", m.Type)
		}
		r.msgs = append(r.msgs, m)
	}
}

// prepareMsgApp constructs a MsgApp message for being sent to the given peer,
// and hands it over to the caller. Updates the replication flow control state
// to account for the fact that the message is about to be sent.
func (r *raft) prepareMsgApp(to pb.PeerID, pr *tracker.Progress, ls LogSlice) pb.Message {
	commit := r.raftLog.committed
	// Update the progress accordingly to the message being sent.
	pr.SentEntries(len(ls.entries), uint64(payloadsSize(ls.entries)))
	pr.MaybeUpdateSentCommit(commit)
	// Hand over the message to the caller.
	return pb.Message{
		From:    r.id,
		To:      to,
		Type:    pb.MsgApp,
		Term:    ls.term,
		Index:   ls.prev.index,
		LogTerm: ls.prev.term,
		Entries: ls.entries,
		Commit:  commit,
		Match:   pr.Match,
	}
}

// maybePrepareMsgApp returns a MsgApp message to be sent to the given peer,
// containing the given log slice.
//
// Returns false if the current state of the node does not permit this MsgApp
// send, e.g. the log slice is misaligned with the replication flow status.
func (r *raft) maybePrepareMsgApp(to pb.PeerID, ls LogSlice) (pb.Message, bool) {
	if r.state != pb.StateLeader || r.Term != ls.term {
		return pb.Message{}, false
	}
	pr := r.trk.Progress(to)
	if pr == nil || pr.State != tracker.StateReplicate || pr.Next != ls.prev.index+1 {
		return pb.Message{}, false
	}
	return r.prepareMsgApp(to, pr, ls), true
}

// maybeSendAppend sends an append RPC with log entries (if any) that are not
// yet known to be replicated in the given peer's log, as well as the current
// commit index. Usually it sends a MsgApp message, but in some cases (e.g. the
// log has been compacted) it can send a MsgSnap.
//
// In some cases, the MsgApp message can have zero entries, and yet be sent.
// When the follower log is not fully up-to-date, we must send a MsgApp
// periodically so that eventually the flow is either accepted or rejected. Not
// doing so can result in replication stall, in cases when a MsgApp is dropped.
//
// Returns true if a message was sent, or false otherwise. A message is not sent
// if the follower log and commit index are up-to-date, the flow is paused (for
// reasons like in-flight limits), or the message could not be constructed.
func (r *raft) maybeSendAppend(to pb.PeerID) bool {
	pr := r.trk.Progress(to)

	last, commit := r.raftLog.lastIndex(), r.raftLog.committed
	sendEntries := pr.ShouldSendEntries(last, r.lazyReplication)
	sendProbe := !sendEntries && pr.ShouldSendProbe(last, commit)
	if !sendEntries && !sendProbe {
		return false
	}

	prevIndex := pr.Next - 1
	prevTerm, err := r.raftLog.term(prevIndex)
	if err != nil {
		// The log probably got truncated at >= pr.Next, so we can't catch up the
		// follower log anymore. Send a snapshot instead.
		return r.maybeSendSnapshot(to, pr)
	}
	var entries []pb.Entry
	if sendEntries {
		if entries, err = r.raftLog.entries(prevIndex, r.maxMsgSize); err != nil {
			// Send a snapshot if we failed to get the entries.
			return r.maybeSendSnapshot(to, pr)
		}
	}

	r.send(r.prepareMsgApp(to, pr, LogSlice{
		term:    r.Term,
		prev:    entryID{index: prevIndex, term: prevTerm},
		entries: entries,
	}))
	return true
}

func (r *raft) sendPing(to pb.PeerID) bool {
	if r.state != pb.StateLeader {
		return false
	}
	pr := r.trk.Progress(to)
	if pr == nil || pr.State != tracker.StateReplicate || pr.MsgAppProbesPaused {
		return false
	}
	prevIndex := pr.Next - 1
	prevTerm, err := r.raftLog.term(prevIndex)
	// An error happens then the log is truncated beyond Next. We can't send a
	// MsgApp in this case, and will soon send a snapshot and enter StateSnapshot.
	if err != nil {
		return false
	}
	// NB: this sets MsgAppProbesPaused to true again.
	r.send(r.prepareMsgApp(to, pr, LogSlice{
		term: r.Term,
		prev: entryID{index: prevIndex, term: prevTerm},
	}))
	return true
}

// maybeSendSnapshot fetches a snapshot from Storage, and sends it to the given
// node. Returns true iff the snapshot message has been emitted successfully.
func (r *raft) maybeSendSnapshot(to pb.PeerID, pr *tracker.Progress) bool {
	if !pr.RecentActive {
		r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
		return false
	}

	snapshot, err := r.raftLog.snapshot()
	if err != nil {
		panic(err) // TODO(pav-kv): handle storage errors uniformly.
	}
	if IsEmptySnap(snapshot) {
		panic("need non-empty snapshot")
	}
	sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
	r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
		r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
	r.becomeSnapshot(pr, sindex)
	r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)

	r.send(pb.Message{To: to, Type: pb.MsgSnap, Snapshot: &snapshot})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *raft) sendHeartbeat(to pb.PeerID) {
	pr := r.trk.Progress(to)
	r.send(pb.Message{
		To:    to,
		Type:  pb.MsgHeartbeat,
		Match: pr.Match,
	})
}

// maybeSendFortify sends a fortification RPC to the given peer if it isn't
// fortified but the peer's store supports the leader's store in StoreLiveness.
func (r *raft) maybeSendFortify(id pb.PeerID) {
	if !r.fortificationTracker.FortificationEnabledForTerm() {
		// The underlying store liveness fabric hasn't been enabled to allow the
		// leader to request support from peers. No-op.
		return
	}

	isFortified, isSupported := r.fortificationTracker.IsFortifiedBy(id)

	if isFortified {
		return // return early if the follower's fortified
	}

	if !isSupported {
		// If the follower isn't providing active store liveness support to the
		// leader, or it is but the leader isn't hearing about it, we don't need to
		// send a fortify message. We will attempt to fortify the follower once
		// store liveness support is established.
		if id == r.id {
			// Log if the leader doesn't support itself in the liveness fabric. This
			// is possible if the leader is affected by disk stalls.
			r.logger.Infof(
				"%x leader at term %d does not support itself in the liveness fabric", r.id, r.Term,
			)
		}

		r.metrics.SkippedFortificationDueToLackOfSupport.Inc(1)
		return
	}

	// Only send a fortify message if we don't know that the follower supports us
	// at the current epoch.
	r.sendFortify(id)
}

// sendFortify sends a fortification RPC to the given peer.
func (r *raft) sendFortify(to pb.PeerID) {
	if to == r.id {
		// We handle the case where the leader is trying to fortify itself specially.
		// Doing so avoids a self-addressed message.
		epoch, live := r.storeLiveness.SupportFor(r.lead)
		if live {
			r.setLeadEpoch(epoch)
			// The leader needs to persist the LeadEpoch durably before it can start
			// supporting itself. We do so by sending a self-addressed
			// MsgFortifyLeaderResp message so that it is added to the msgsAfterAppend
			// slice and delivered back to this node only after LeadEpoch is
			// persisted. At that point, this node can record support without
			// discrimination for who is providing support (itself vs. other
			// follower).
			r.send(pb.Message{To: r.id, Type: pb.MsgFortifyLeaderResp, LeadEpoch: epoch})
		}
		return
	}
	r.send(pb.Message{To: to, Type: pb.MsgFortifyLeader})
}

// sendDeFortify sends a de-fortification RPC to the given peer.
func (r *raft) sendDeFortify(to pb.PeerID) {
	fortifiedTerm := r.fortificationTracker.Term()
	if to == r.id {
		// We handle the case where the leader is trying to de-fortify itself
		// specially. Doing so avoids a self-addressed message.
		switch {
		case r.Term == fortifiedTerm:
			r.deFortify(r.id, fortifiedTerm)
		case r.Term > fortifiedTerm:
			// NB: The current term has advanced, so we don't attempt to de-fortify,
			// as the de-fortification attempt corresponds to a prior term. These term
			// checks would have happened in Step had we sent a self-addressed
			// message; we decided not to, so we need to handle this especially
			// ourselves.
			r.logger.Debugf("de-foritfying self at term %d is a no-op; current term %d",
				fortifiedTerm, r.Term,
			)
		case r.Term < fortifiedTerm:
			panic("fortification tracker's term cannot be higher than raft group's")
		}
		return
	}
	r.send(pb.Message{To: to, Type: pb.MsgDeFortifyLeader, Term: fortifiedTerm})
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.trk.
func (r *raft) bcastAppend() {
	r.trk.Visit(func(id pb.PeerID, _ *tracker.Progress) {
		if id == r.id {
			// NB: the leader doesn't send MsgApp to itself here nor does it receive
			// a self directed MsgAppResp. This means that the leader will not have a
			// chance to update its own MatchCommit/SentCommit. That is fine because
			// the leader doesn't use MatchCommit/SentCommit for itself. It only uses
			// the peers' values.
			return
		}
		r.maybeSendAppend(id)
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	r.trk.Visit(func(id pb.PeerID, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id)
	})
}

// bcastFortify attempts to send an RPC to fortify the leader to all the peers
// (including the leader itself) whose stores are currently providing store
// liveness support to the leader's store but who have not fortified the leader.
func (r *raft) bcastFortify() {
	assertTrue(r.state == pb.StateLeader, "only leaders can fortify")
	r.trk.Visit(func(id pb.PeerID, _ *tracker.Progress) {
		r.maybeSendFortify(id)
	})
}

// bcastDeFortify attempts to de-fortify the current peer's last (post restart)
// leadership term by sending an RPC to all peers (including itself).
func (r *raft) bcastDeFortify() {
	assertTrue(r.state != pb.StateLeader, "leaders should not de-fortify without stepping down")
	assertTrue(r.fortificationTracker.CanDefortify(), "unsafe to de-fortify")

	r.trk.Visit(func(id pb.PeerID, _ *tracker.Progress) {
		r.sendDeFortify(id)
	})
}

// shouldBCastDeFortify returns whether we should attempt to broadcast a
// MsgDeFortifyLeader to all peers or not.
func (r *raft) shouldBcastDeFortify() bool {
	assertTrue(r.state != pb.StateLeader, "leaders should not be de-fortifying without stepping down")

	if !r.fortificationTracker.NeedsDefortify() {
		// Fast path for if we've determined we no longer need to de-fortify.
		return false
	}
	if !r.fortificationTracker.CanDefortify() {
		return false
	}

	committedTerm, err := r.raftLog.term(r.raftLog.committed)
	if err != nil {
		// NB: Extremely rare case where we can't figure out what the currently
		// committed term is. Conservatively send out a MsgDeFortifyLeader, but log
		// loudly.
		r.logger.Warningf("failed to get committed term: %v", err)
		return true
	}

	// Inform the fortification tracker of the committed term, and see whether the
	// answer to whether we should de-fortify has changed.
	r.fortificationTracker.InformCommittedTerm(committedTerm)
	return r.fortificationTracker.NeedsDefortify()
}

// maybeUnpauseAndBcastAppend unpauses and attempts to send an MsgApp to all the
// followers that provide store liveness support. If there is no store liveness
// support, we skip unpausing and sending MsgApp because the message is likely
// to be dropped.
func (r *raft) maybeUnpauseAndBcastAppend() {
	r.trk.Visit(func(id pb.PeerID, pr *tracker.Progress) {
		if r.id == id {
			// NB: the leader doesn't send MsgApp to itself here nor does it receive
			// a self directed MsgAppResp. This means that the leader will not have a
			// chance to update its own MatchCommit/SentCommit. That is fine because
			// the leader doesn't use MatchCommit/SentCommit for itself. It only uses
			// the peers' values.
			return
		}

		if _, supported := r.fortificationTracker.IsFortifiedBy(id); !supported {
			// If the follower's store isn't providing active store liveness support
			// to the leader's store, or it is but the leader isn't hearing about it,
			// we don't send a MsgApp.
			return
		}

		pr.MsgAppProbesPaused = false
		r.maybeSendAppend(id)
	})
}

func (r *raft) appliedTo(index uint64, size entryEncodingSize) {
	oldApplied := r.raftLog.applied
	newApplied := max(index, oldApplied)
	r.raftLog.appliedTo(newApplied, size)

	if r.config.AutoLeave && newApplied >= r.pendingConfIndex && r.state == pb.StateLeader {
		// If the current (and most recent, at least for this leader's term)
		// configuration should be auto-left, initiate that now. We use a
		// nil Data which unmarshals into an empty ConfChangeV2 and has the
		// benefit that appendEntry can never refuse it based on its size
		// (which registers as zero).
		m, err := confChangeToMsg(nil)
		if err != nil {
			panic(err)
		}
		// NB: this proposal can't be dropped due to size, but can be
		// dropped if a leadership transfer is in progress. We'll keep
		// checking this condition on each applied entry, so either the
		// leadership transfer will succeed and the new leader will leave
		// the joint configuration, or the leadership transfer will fail,
		// and we will propose the config change on the next advance.
		if err := r.Step(m); err != nil {
			r.logger.Debugf("not initiating automatic transition out of joint configuration %s: %v", r.config, err)
		} else {
			r.logger.Infof("initiating automatic transition out of joint configuration %s", r.config)
		}
	}
}

func (r *raft) appliedSnap(snap *pb.Snapshot) {
	index := snap.Metadata.Index
	r.raftLog.stableSnapTo(index)
	r.appliedTo(index, 0 /* size */)
}

// maybeCommit attempts to advance the commit index. Returns true if the commit
// index changed (in which case the caller should call r.bcastAppend). This can
// only be called in StateLeader.
func (r *raft) maybeCommit() bool {
	index := r.trk.Committed()
	if index <= r.raftLog.committed {
		// The commit index must not regress.
		// TODO(pav-kv): consider making it an assertion.
		return false
	}
	// Section 5.4.2 of raft paper (https://raft.github.io/raft.pdf):
	//
	// Only log entries from the leader’s current term are committed by counting
	// replicas; once an entry from the current term has been committed in this
	// way, then all prior entries are committed indirectly because of the Log
	// Matching Property.
	//
	// This comparison is equivalent in output to:
	// if !r.raftLog.matchTerm(entryID{term: r.Term, index: index})
	// But avoids (potentially) loading the entry term from storage.
	if index <= r.idxPreLeading {
		return false
	}

	r.raftLog.commitTo(LogMark{Term: r.Term, Index: index})
	return true
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		// NB: There are state transitions where reset may be called on a follower
		// that supports a fortified leader. One example is when a follower is
		// supporting the old leader, but the quorum isn't, so a new leader is
		// elected. this case, the follower that's supporting the old leader will
		// eventually hear from the new leader and call reset with the new leader's
		// term. Naively, this would trip this assertion -- however, in this
		// case[*], we expect a call to deFortify first.
		//
		// [*] this case, and other cases where a state transition implies
		// de-fortification.
		assertTrue(!r.supportingFortifiedLeader(),
			"should not be changing terms when supporting a fortified leader")
		r.setTerm(term)
	}

	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.electionTracker.ResetVotes()
	r.trk.Visit(func(id pb.PeerID, pr *tracker.Progress) {
		*pr = tracker.Progress{
			Match:       0,
			MatchCommit: 0,
			Next:        r.raftLog.lastIndex() + 1,
			Inflights:   tracker.NewInflights(r.maxInflight, r.maxInflightBytes),
			IsLearner:   pr.IsLearner,
		}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	})

	r.pendingConfIndex = 0
	r.uncommittedSize = 0
}

func (r *raft) setTerm(term uint64) {
	if term == r.Term {
		return
	}
	assertTrue(term > r.Term, "term cannot regress")
	r.Term = term
	r.Vote = None
	r.resetLead()
}

func (r *raft) setVote(id pb.PeerID) {
	if id == r.Vote {
		return
	}
	assertTrue(r.Vote == None, "cannot change vote")
	r.Vote = id
}

func (r *raft) setLead(lead pb.PeerID) {
	if lead == r.lead {
		return
	}
	assertTrue(r.lead == None, "cannot change lead")
	r.lead = lead
}

func (r *raft) resetLead() {
	r.lead = None
	r.resetLeadEpoch()
}

func (r *raft) setLeadEpoch(leadEpoch pb.Epoch) {
	if leadEpoch == r.leadEpoch {
		return
	}
	assertTrue(r.lead != None, "leader must be set")
	assertTrue(leadEpoch > r.leadEpoch, "leadEpoch cannot regress")
	r.leadEpoch = leadEpoch
}

func (r *raft) resetLeadEpoch() {
	r.leadEpoch = 0
}

func (r *raft) appendEntry(es ...pb.Entry) (accepted bool) {
	last := r.raftLog.lastEntryID()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = last.index + 1 + uint64(i)
	}
	// Track the size of this uncommitted proposal.
	if !r.increaseUncommittedSize(es) {
		r.logger.Warningf(
			"%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
			r.id,
		)
		// Drop the proposal.
		return false
	}
	app := LogSlice{term: r.Term, prev: last, entries: es}
	if err := app.valid(); err != nil {
		r.logger.Panicf("%x leader could not append to its log: %v", r.id, err)
	} else if !r.raftLog.append(app) {
		r.logger.Panicf("%x leader could not append to its log", r.id)
	}

	// On appending entries, the leader is effectively "sending" a MsgApp to its
	// local "acceptor". Since we don't actually send this self-MsgApp, update the
	// progress here as if it was sent.
	r.trk.Progress(r.id).Next = app.lastIndex() + 1
	// The leader needs to self-ack the entries just appended once they have
	// been durably persisted (since it doesn't send an MsgApp to itself). This
	// response message will be added to msgsAfterAppend and delivered back to
	// this node after these entries have been written to stable storage. When
	// handled, this is roughly equivalent to:
	//
	//  r.trk.Progress[r.id].MaybeUpdate(e.Index)
	//  if r.maybeCommit() {
	//  	r.bcastAppend()
	//  }
	r.send(pb.Message{To: r.id, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex(),
		Commit: r.raftLog.committed})
	return true
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	assertTrue(r.state != pb.StateLeader, "tickElection called by leader")

	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0

		if r.shouldBcastDeFortify() {
			r.bcastDeFortify()
		}
	}

	if r.leadEpoch != 0 {
		assertTrue(r.electionElapsed == 0, "fortifying followers don't set electionElapsed")
		if r.supportingFortifiedLeader() {
			// There's a fortified leader and we're supporting it.
			return
		}
		// We're no longer supporting the fortified leader. Calling r.deFortify()
		// will forward the electionElapsed to electionTimeout which means that
		// this peer can immediately vote in elections. Moreover, r.deFortify()
		// will reset leadEpoch to 0 which ensures that we don't enter this
		// conditional block again unless the term changes or the follower is
		// re-fortified. This means we'll only ever skip the initial part of the
		// election timeout once per fortified -> no longer fortified transition.
		r.deFortify(r.id, r.Term)
	} else {
		r.electionElapsed++
	}

	if r.atRandomizedElectionTimeout() {
		if err := r.Step(pb.Message{From: r.id, Type: pb.MsgHup}); err != nil {
			r.logger.Debugf("error occurred during election: %v", err)
		}
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	assertTrue(r.state == pb.StateLeader, "tickHeartbeat called by non-leader")

	// Compute the LeadSupportUntil on every tick.
	r.fortificationTracker.ComputeLeadSupportUntil(r.state)

	// Check if we intended to step down. If so, step down if it's safe to do so.
	// Otherwise, continue doing leader things.
	if r.fortificationTracker.SteppingDown() && r.fortificationTracker.CanDefortify() {
		r.becomeFollower(r.fortificationTracker.SteppingDownTerm(), None)
		return
	}

	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			r.checkQuorumActive()
			if r.state != pb.StateLeader {
				return // stepped down
			}
		}
		// If current leader cannot transfer leadership in electionTimeout, it stops
		// trying and begins accepting new proposals again.
		if r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0

		if r.fortificationTracker.FortificationEnabledForTerm() {
			// Mark fortifying followers as recently active. We disable heartbeats
			// when leader fortification is enabled, instead deferring to
			// StoreLiveness for failure detection. As such, if there is no append
			// activity for a raft group, it's possible for the leader to not
			// communicate with a follower within an electionTimeout. We do not want
			// to infer that the leader can't communicate with a follower in such
			// cases, which could then cause the leader to spuriously step down
			// because of CheckQuorum. Instead, we compute RecentlyActive based on
			// StoreLiveness instead.
			r.markFortifyingFollowersAsRecentlyActive()

			// Try to refortify any followers that currently support us in
			// StoreLiveness but aren't fortified.
			r.bcastFortify()
			r.maybeUnpauseAndBcastAppend()
		} else {
			// Leader fortification isn't enabled, so we rely on sending out periodic
			// heartbeats to all followers to prevent them from calling an election.
			if err := r.Step(pb.Message{From: r.id, Type: pb.MsgBeat}); err != nil {
				r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
			}
		}
	}
}

func (r *raft) becomeFollower(term uint64, lead pb.PeerID) {
	assertTrue(lead != r.id, "should not be stepping down to a follower when the lead field points to us")

	r.state = pb.StateFollower
	r.step = stepFollower
	r.tick = r.tickElection

	// Start de-fortifying eagerly so we don't have to wait out a full heartbeat
	// timeout before sending the first de-fortification message.
	if r.shouldBcastDeFortify() {
		r.bcastDeFortify()
	}

	r.reset(term)
	r.setLead(lead)
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
	r.logger.Debugf("%x reset election elapsed to %d", r.id, r.electionElapsed)
}

func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == pb.StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	// We vote for ourselves when we become a candidate. We shouldn't do so if
	// we're already fortified.
	assertTrue(!r.supportingFortifiedLeader(), "shouldn't become a candidate if we're supporting a fortified leader")
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.setVote(r.id)
	r.state = pb.StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

func (r *raft) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == pb.StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	assertTrue(!r.supportingFortifiedLeader(), "should not be fortifying a leader when becoming pre-candidate")
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	r.step = stepCandidate
	r.electionTracker.ResetVotes()
	r.tick = r.tickElection
	// TODO(arul): We're forgetting the raft leader here. From the perspective of
	// leader leases, this is fine, because we wouldn't be here unless we'd
	// revoked StoreLiveness support for the leader's store to begin with. It's
	// a bit weird from the perspective of raft though. See if we can avoid this.
	r.resetLead()
	r.state = pb.StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == pb.StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	// NB: The fortificationTracker holds state from a peer's leadership stint
	// that's acted upon after it has stepped down. We reset it right before
	// stepping up to become leader again, but not when stepping down as leader
	// and not even when learning of a leader in a later term.
	r.fortificationTracker.Reset(r.Term)
	r.tick = r.tickHeartbeat
	r.setLead(r.id)
	r.state = pb.StateLeader
	// TODO(pav-kv): r.reset already scans the peers. Try avoiding another scan.
	r.trk.Visit(func(id pb.PeerID, pr *tracker.Progress) {
		if id == r.id {
			// Followers enter replicate mode when they've been successfully probed
			// (perhaps after having received a snapshot as a result). The leader is
			// trivially in this state. Note that r.reset() has initialized this
			// progress with the last index already.
			r.becomeReplicate(pr)
			// The leader always has RecentActive == true. The checkQuorumActive
			// method makes sure to preserve this.
			pr.RecentActive = true
			return
		}
		// All peer flows, except the leader's own, are initially in StateProbe.
		// Account the probe state entering in metrics here. All subsequent flow
		// state changes, while we are the leader, are counted in the corresponding
		// methods: becomeProbe, becomeReplicate, becomeSnapshot.
		assertTrue(pr.State == tracker.StateProbe, "peers must be in StateProbe on leader step up")
		r.metrics.FlowsEnteredStateProbe.Inc(1)
	})

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	r.pendingConfIndex = r.raftLog.lastIndex()

	// Remember the last log index before the term advances to
	// our current (leader) term.
	// See the idxPreLeading comment for more details.
	r.idxPreLeading = r.raftLog.lastIndex()

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		// This won't happen because we just called reset() above.
		r.logger.Panic("empty entry was dropped")
	}
	// The payloadSize of an empty entry is 0 (see TestPayloadSizeOfEmptyEntry),
	// so the preceding log append does not count against the uncommitted log
	// quota of the new leader. In other words, after the call to appendEntry,
	// r.uncommittedSize is still 0.
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *raft) becomeProbe(pr *tracker.Progress) {
	r.metrics.FlowsEnteredStateProbe.Inc(1)
	pr.BecomeProbe()
}

func (r *raft) becomeReplicate(pr *tracker.Progress) {
	r.metrics.FlowsEnteredStateReplicate.Inc(1)
	pr.BecomeReplicate()
}

func (r *raft) becomeSnapshot(pr *tracker.Progress, index uint64) {
	r.metrics.FlowsEnteredStateSnapshot.Inc(1)
	pr.BecomeSnapshot(index)
}

func (r *raft) hup(t CampaignType) {
	if r.state == pb.StateLeader {
		r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		return
	}
	if !r.promotable() {
		r.logger.Infof("%x is unpromotable and can not campaign", r.id)
		return
	}
	// NB: Even an old leader that has since stepped down needs to ensure it is
	// no longer fortifying itself before campaigning at a higher term. This is
	// because candidates always vote for themselves, and casting a vote isn't
	// allowed if the candidate is fortifying a leader. So, before campaigning,
	// the old leader needs to either de-fortify itself[1] or withdraw store
	// liveness support for its own store for the fortified epoch[2].
	//
	// [1] The old leader that's since stepped down will only de-fortify itself
	// once its leadMaxSupported is in the past. Waiting until we're
	// de-fortified before campaigning ensures we don't regress the
	// leadMaxSupported.
	//
	// [2] While rare, this can happen if it experiences a disk stall. In this
	// case, the leadMaxSupported may be in the future by the time we campaign.
	// However, if this is the case, we won't be able to win the election, as a
	// majority quorum will not vote for us because they'll be in fortify lease.
	if r.supportingFortifiedLeader() {
		r.logger.Debugf("%x ignoring MsgHup due to leader fortification", r.id)
		return
	}

	// We shouldn't campaign if we don't have quorum support in store liveness. We
	// only make an exception if this is a leadership transfer, because otherwise
	// the transfer might fail if the new leader doesn't already have support.
	if t != campaignTransfer && r.fortificationTracker.RequireQuorumSupportOnCampaign() &&
		!r.fortificationTracker.QuorumSupported() &&
		!r.testingKnobs.IsPreCampaignStoreLivenessCheckDisabled() {
		r.logger.Debugf("%x cannot campaign since it's not supported by a quorum in store liveness", r.id)
		return
	}

	if r.hasUnappliedConfChanges() {
		r.logger.Warningf("%x cannot campaign at term %d since there are still pending configuration changes to apply", r.id, r.Term)
		return
	}

	r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
	r.campaign(t)
}

// supportingFortifiedLeader returns whether the peer is providing fortification
// support to a leader. When a peer is providing support to a leader, it must
// not campaign or vote to disrupt that leader's term, unless specifically asked
// to do so by the leader.
func (r *raft) supportingFortifiedLeader() bool {
	if r.leadEpoch == 0 {
		return false // not supporting any leader
	}
	assertTrue(r.lead != None, "lead epoch is set but leader is not")
	epoch, live := r.storeLiveness.SupportFor(r.lead)
	assertTrue(epoch >= r.leadEpoch, "epochs in store liveness shouldn't regress")
	return live && epoch == r.leadEpoch
}

// errBreak is a sentinel error used to break a callback-based loop.
var errBreak = errors.New("break")

func (r *raft) hasUnappliedConfChanges() bool {
	if r.raftLog.applied >= r.raftLog.committed { // in fact applied == committed
		return false
	}
	found := false
	// Scan all unapplied committed entries to find a config change. Paginate the
	// scan, to avoid a potentially unlimited memory spike.
	lo, hi := r.raftLog.applied, r.raftLog.committed
	// Reuse the maxApplyingEntsSize limit because it is used for similar purposes
	// (limiting the read of unapplied committed entries) when raft sends entries
	// via the Ready struct for application.
	// TODO(pavelkalinnikov): find a way to budget memory/bandwidth for this scan
	// outside the raft package.
	pageSize := r.raftLog.maxApplyingEntsSize
	if err := r.raftLog.scan(lo, hi, pageSize, func(ents []pb.Entry) error {
		for i := range ents {
			if ents[i].Type == pb.EntryConfChange || ents[i].Type == pb.EntryConfChangeV2 {
				found = true
				return errBreak
			}
		}
		return nil
	}); err != nil && err != errBreak {
		r.logger.Panicf("error scanning unapplied entries (%d, %d]: %v", lo, hi, err)
	}
	return found
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
func (r *raft) campaign(t CampaignType) {
	if !r.promotable() {
		// This path should not be hit (callers are supposed to check), but
		// better safe than sorry.
		r.logger.Warningf("%x is unpromotable; campaign() should have been called", r.id)
	}
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}
	ids := maps.Keys(r.config.Voters.IDs())
	slices.Sort(ids)
	for _, id := range ids {
		if id == r.id {
			// The candidate votes for itself and should account for this self
			// vote once the vote has been durably persisted (since it doesn't
			// send a MsgVote to itself). This response message will be added to
			// msgsAfterAppend and delivered back to this node after the vote
			// has been written to stable storage.
			r.send(pb.Message{To: id, Term: term, Type: voteRespMsgType(voteMsg)})
			continue
		}
		// TODO(pav-kv): it should be ok to simply print %+v for the lastEntryID.
		last := r.raftLog.lastEntryID()
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, last.term, last.index, voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{To: id, Term: term, Type: voteMsg, Index: last.index, LogTerm: last.term, Context: ctx})
	}
}

func (r *raft) poll(
	id pb.PeerID, t pb.MessageType, v bool,
) (granted int, rejected int, result quorum.VoteResult) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	r.electionTracker.RecordVote(id, v)
	return r.electionTracker.TallyVotes()
}

func (r *raft) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if IsMsgIndicatingLeader(m.Type) {
			// We've just received a message that indicates that a new leader was
			// elected at a higher term, but the message may not be from the leader
			// itself. Either way, the old leader is no longer fortified, so it's safe
			// to de-fortify at this point.
			r.logMsgHigherTerm(m, "new leader indicated, advancing term")
			r.deFortify(m.From, m.Term)
			var lead pb.PeerID
			if IsMsgFromLeader(m.Type) {
				lead = m.From
			}
			r.becomeFollower(m.Term, lead)
		} else {
			// We've just received a message that does not indicate that a new leader
			// was elected at a higher term. All it means is that some other peer may
			// be operating at this term.

			// If a server receives a message (with any type) but is still supporting
			// a fortified leader ("in a leader lease"), it does not update its term,
			// grant a vote, or process the message in any other way.
			if r.inLeaderLease(m) {
				// However, since we're in a leader lease and have received a message at
				// a higher term, we may need to take action to recover a stranded peer
				// by catching the quorum's term up to the peer's.
				//
				// For legacy reasons, we don't consider a MsgVote at a higher term to
				// be a reason to advance the term to recover a stranded peer. Instead,
				// we wait for the leader's heartbeat message to be rejected by the
				// stranded peer, which will cause the leader to step down.
				//
				// TODO(nvanbenschoten): remove this special case. If we see a MsgVote
				// from a peer at a higher term while we are in a leader lease, we should
				// still step down.
				strandedPeer := senderHasMsgTerm(m) && m.Type != pb.MsgVote
				if strandedPeer {
					// If we are supporting a fortified leader, we can't just jump to the
					// larger term. Instead, we need to wait for the leader to learn about
					// the stranded peer, step down, and defortify. The only thing to do
					// here is check whether we are that leader, in which case we should
					// step down to kick off this process of catching up to the term of
					// the stranded peer.
					//
					// However, the leader can't just blindly step down and defortify.
					// Doing so could cause LeadSupportUntil to regress. Instead, the
					// leader checks if it's safe to step down (it can defortify). If it
					// is, it will step down. Otherwise, it will mark its intention to
					// step down. This will freeze LeadSupportUntil[1], and eventually the
					// leader will be able to safely step down.
					//
					// [1] When freezing the LeadSupportUntil, we'll also track the term
					// of the stranded follower. This then allows us to become a follower
					// at that term and campaign at a term higher to allow the stranded
					// follower to rejoin. Had we not done this, we may have had to
					// campaign at a lower term, and then know about the higher term, and
					// then campaign again at the higher term.
					if r.state == pb.StateLeader {
						if r.fortificationTracker.CanDefortify() {
							r.logMsgHigherTerm(m, "stepping down as leader to recover stranded peer")
							r.deFortify(r.id, m.Term)
							r.becomeFollower(m.Term, None)
						} else {
							r.logMsgHigherTerm(m, "intending to step down as leader to recover stranded peer")
							r.fortificationTracker.BeginSteppingDown(m.Term)
						}
					} else {
						r.logMsgHigherTerm(m, "ignoring and still supporting fortified leader")
					}
				}
				return nil
			}
			// If we are willing process a message at a higher term, then make sure we
			// record that we have withdrawn our support for the current leader, if we
			// were still providing it with fortification support up to this point.
			r.deFortify(m.From, m.Term)

			// If the message indicates a higher term, we should update our term and
			// step down to a follower.
			if senderHasMsgTerm(m) {
				r.logMsgHigherTerm(m, "advancing term")
				r.becomeFollower(m.Term, None)
			}
		}

	case m.Term < r.Term:
		ignore := true

		switch m.Type {
		case pb.MsgHeartbeat, pb.MsgApp, pb.MsgFortifyLeader:
			if !r.checkQuorum && !r.preVote {
				break
			}
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this node's activeness.
			// The above comments also true for Pre-Vote
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "pb.MsgAppResp" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck node with
			// fresh election. This can be prevented with Pre-Vote phase.
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
			return nil

		case pb.MsgPreVote:
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			last := r.raftLog.lastEntryID()
			// TODO(pav-kv): it should be ok to simply print %+v of the lastEntryID.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, last.term, last.index, r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: pb.MsgPreVoteResp, Reject: true})
			return nil

		case pb.MsgSnap:
			// A snapshot message may arrive under an outdated term. Since it carries
			// committed state, we can safely process it regardless of the term. The
			// message term means the snapshot is committed as of this term. By raft
			// invariants, all committed state under a particular term will be
			// committed under later terms as well.
			//
			// TODO(#127348): the MsgSnap handler assumes the message came from this
			// term leader, which is not true if the term is bumped here.
			// TODO(#127349): it is generally not true because the snapshot could have
			// been initiated by a leaseholder (which at the time of writing is not
			// necessarily the leader), and/or delegated via a follower.
			m.Term = r.Term
			ignore = false
		}

		// Ignore the message if it has not been handled above and can not be
		// handled below.
		if ignore {
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			return nil
		}
	}

	switch m.Type {
	case pb.MsgHup:
		if r.preVote {
			r.hup(campaignPreElection)
		} else {
			r.hup(campaignElection)
		}

	case pb.MsgStorageAppendResp:
		// The snapshot precedes the entries. We acknowledge the snapshot first,
		// then the entries, as required by the unstable structure.
		if m.Snapshot != nil {
			r.appliedSnap(m.Snapshot)
		}
		if m.Index != 0 {
			r.raftLog.stableTo(LogMark{Term: m.LogTerm, Index: m.Index})
		}

	case pb.MsgStorageApplyResp:
		if len(m.Entries) > 0 {
			index := m.Entries[len(m.Entries)-1].Index
			r.appliedTo(index, entsSize(m.Entries))
			r.reduceUncommittedSize(payloadsSize(m.Entries))
		}

	case pb.MsgVote, pb.MsgPreVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.Vote == m.From ||
			// ...OR we haven't voted and we don't think there's a leader yet in this
			// term...
			(r.Vote == None && r.lead == None) ||
			// ...OR this is a PreVote for a future term...
			(m.Type == pb.MsgPreVote && m.Term > r.Term)
		// ...AND we believe the candidate is up to date.
		lastID := r.raftLog.lastEntryID()
		candLastID := entryID{term: m.LogTerm, index: m.Index}
		if canVote && r.raftLog.isUpToDate(candLastID) {
			// Note: it turns out that that learners must be allowed to cast votes.
			// This seems counter- intuitive but is necessary in the situation in which
			// a learner has been promoted (i.e. is now a voter) but has not learned
			// about this yet.
			// For example, consider a group in which id=1 is a learner and id=2 and
			// id=3 are voters. A configuration change promoting 1 can be committed on
			// the quorum `{2,3}` without the config change being appended to the
			// learner's log. If the leader (say 2) fails, there are de facto two
			// voters remaining. Only 3 can win an election (due to its log containing
			// all committed entries), but to do so it will need 1 to vote. But 1
			// considers itself a learner and will continue to do so until 3 has
			// stepped up as leader, replicates the conf change to 1, and 1 applies it.
			// Ultimately, by receiving a request to vote, the learner realizes that
			// the candidate believes it to be a voter, and that it should act
			// accordingly. The candidate's config may be stale, too; but in that case
			// it won't win the election, at least in the absence of the bug discussed
			// in:
			// https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, lastID.term, lastID.index, r.Vote, m.Type, m.From, candLastID.term, candLastID.index, r.Term)
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why, consider the
			// case where a single node was previously partitioned away and
			// it's local term is now out of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				r.setVote(m.From)
			}
		} else {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, lastID.term, lastID.index, r.Vote, m.Type, m.From, candLastID.term, candLastID.index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		err := r.step(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

// inLeaderLease returns whether the message should be ignored because the
// recipient is supporting a leader lease.
func (r *raft) inLeaderLease(m pb.Message) bool {
	force := bytes.Equal(m.Context, []byte(campaignTransfer))
	if force {
		// If the message is a transfer request, we ignore the leader lease.
		return false
	}

	inHeartbeatLease := r.checkQuorum &&
		// We know who the leader is...
		r.lead != None &&
		// And we're not fortifying the leader (else we'd be in a fortify lease, see
		// below)...
		r.leadEpoch == 0 &&
		// And we've heard from the leader recently...
		r.electionElapsed < r.electionTimeout &&
		// And the message is a pre-vote or vote request. Unlike a fortify lease,
		// a heartbeat lease only applies to these two message types.
		(m.Type == pb.MsgPreVote || m.Type == pb.MsgVote)

	inFortifyLease := r.supportingFortifiedLeader() &&
		// NB: If the peer that's campaigning has an entry in its log with a
		// higher term than what we're aware of, then this conclusively proves
		// that a new leader was elected at a higher term. We never heard from
		// this new leader (otherwise we'd have bumped r.Term in response).
		// However, any fortification we're providing to a leader that has been
		// since dethroned is pointless.
		m.LogTerm <= r.Term

	if !inHeartbeatLease && !inFortifyLease {
		// We're not in either form of lease. We can safely process the message.
		return false
	}

	// Log why we're ignoring the message.
	var leaseMsg redact.RedactableString
	if inHeartbeatLease {
		leaseMsg = redact.Sprintf("recently received communication from leader (remaining ticks: %d)", r.electionTimeout-r.electionElapsed)
	} else {
		leaseMsg = redact.Sprintf("supporting fortified leader %d at epoch %d", r.lead, r.leadEpoch)
	}
	last := r.raftLog.lastEntryID()
	// TODO(pav-kv): it should be ok to simply print the %+v of the lastEntryID.
	r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: %s",
		r.id, last.term, last.index, r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, leaseMsg)
	return true
}

func (r *raft) logMsgHigherTerm(m pb.Message, suffix redact.SafeString) {
	r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d], %s",
		r.id, r.Term, m.Type, m.From, m.Term, suffix)
}

type stepFunc func(r *raft, m pb.Message) error

func stepLeader(r *raft, m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MsgProp:
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		if r.trk.Progress(r.id) == nil {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		for i := range m.Entries {
			e := &m.Entries[i]
			var cc pb.ConfChangeI
			if e.Type == pb.EntryConfChange {
				var ccc pb.ConfChange
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			} else if e.Type == pb.EntryConfChangeV2 {
				var ccc pb.ConfChangeV2
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			}
			if cc != nil {
				ccCtx := confchange.ValidationContext{
					CurConfig:                         &r.config,
					Applied:                           r.raftLog.applied,
					PendingConfIndex:                  r.pendingConfIndex,
					LeadSupportSafe:                   r.fortificationTracker.ConfigChangeSafe(),
					DisableValidationAgainstCurConfig: r.disableConfChangeValidation,
				}
				if err := confchange.ValidateProp(ccCtx, cc.AsV2()); err != nil {
					r.logger.Infof("%x ignoring conf change %v at config %s: %s", r.id, cc, r.config, err)
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else {
					r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
				}
			}
		}

		if !r.appendEntry(m.Entries...) {
			return ErrProposalDropped
		}
		r.bcastAppend()
		return nil

	case pb.MsgForgetLeader:
		return nil // noop on leader
	}

	// All other message types require a progress for m.From (pr).
	pr := r.trk.Progress(m.From)
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.Type {
	case pb.MsgAppResp:
		// NB: this code path is also hit from (*raft).advance, where the leader steps
		// an MsgAppResp to acknowledge the appended entries in the last Ready.

		pr.RecentActive = true
		pr.MaybeUpdateMatchCommit(m.Commit)
		if m.Reject {
			// RejectHint is the suggested next base entry for appending (i.e.
			// we try to append entry RejectHint+1 next), and LogTerm is the
			// term that the follower has at index RejectHint. Older versions
			// of this library did not populate LogTerm for rejections and it
			// is zero for followers with an empty log.
			//
			// Under normal circumstances, the leader's log is longer than the
			// follower's and the follower's log is a prefix of the leader's
			// (i.e. there is no divergent uncommitted suffix of the log on the
			// follower). In that case, the first probe reveals where the
			// follower's log ends (RejectHint=follower's last index) and the
			// subsequent probe succeeds.
			//
			// However, when networks are partitioned or systems overloaded,
			// large divergent log tails can occur. The naive attempt, probing
			// entry by entry in decreasing order, will be the product of the
			// length of the diverging tails and the network round-trip latency,
			// which can easily result in hours of time spent probing and can
			// even cause outright outages. The probes are thus optimized as
			// described below.
			r.logger.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d",
				r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
			nextProbeIdx := m.RejectHint
			if m.LogTerm > 0 {
				// If the follower has an uncommitted log tail, we would end up
				// probing one by one until we hit the common prefix.
				//
				// For example, if the leader has:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 5 5 5 5 5
				//   term (F)   1 1 1 1 2 2
				//
				// Then, after sending an append anchored at (idx=9,term=5) we
				// would receive a RejectHint of 6 and LogTerm of 2. Without the
				// code below, we would try an append at index 6, which would
				// fail again.
				//
				// However, looking only at what the leader knows about its own
				// log and the rejection hint, it is clear that a probe at index
				// 6, 5, 4, 3, and 2 must fail as well:
				//
				// For all of these indexes, the leader's log term is larger than
				// the rejection's log term. If a probe at one of these indexes
				// succeeded, its log term at that index would match the leader's,
				// i.e. 3 or 5 in this example. But the follower already told the
				// leader that it is still at term 2 at index 6, and since the
				// log term only ever goes up (within a log), this is a contradiction.
				//
				// At index 1, however, the leader can draw no such conclusion,
				// as its term 1 is not larger than the term 2 from the
				// follower's rejection. We thus probe at 1, which will succeed
				// in this example. In general, with this approach we probe at
				// most once per term found in the leader's log.
				//
				// There is a similar mechanism on the follower (implemented in
				// handleAppendEntries via a call to findConflictByTerm) that is
				// useful if the follower has a large divergent uncommitted log
				// tail[1], as in this example:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 3 3 3 3 7
				//   term (F)   1 3 3 4 4 5 5 5 6
				//
				// Naively, the leader would probe at idx=9, receive a rejection
				// revealing the log term of 6 at the follower. Since the leader's
				// term at the previous index is already smaller than 6, the leader-
				// side optimization discussed above is ineffective. The leader thus
				// probes at index 8 and, naively, receives a rejection for the same
				// index and log term 5. Again, the leader optimization does not improve
				// over linear probing as term 5 is above the leader's term 3 for that
				// and many preceding indexes; the leader would have to probe linearly
				// until it would finally hit index 3, where the probe would succeed.
				//
				// Instead, we apply a similar optimization on the follower. When the
				// follower receives the probe at index 8 (log term 3), it concludes
				// that all of the leader's log preceding that index has log terms of
				// 3 or below. The largest index in the follower's log with a log term
				// of 3 or below is index 3. The follower will thus return a rejection
				// for index=3, log term=3 instead. The leader's next probe will then
				// succeed at that index.
				//
				// [1]: more precisely, if the log terms in the large uncommitted
				// tail on the follower are larger than the leader's. At first,
				// it may seem unintuitive that a follower could even have such
				// a large tail, but it can happen:
				//
				// 1. Leader appends (but does not commit) entries 2 and 3, crashes.
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 2 2     [crashes]
				//   term (F)   1
				//   term (F)   1
				//
				// 2. a follower becomes leader and appends entries at term 3.
				//              -----------------
				//   term (x)   1 2 2     [down]
				//   term (F)   1 3 3 3 3
				//   term (F)   1
				//
				// 3. term 3 leader goes down, term 2 leader returns as term 4
				//    leader. It commits the log & entries at term 4.
				//
				//              -----------------
				//   term (L)   1 2 2 2
				//   term (x)   1 3 3 3 3 [down]
				//   term (F)   1
				//              -----------------
				//   term (L)   1 2 2 2 4 4 4
				//   term (F)   1 3 3 3 3 [gets probed]
				//   term (F)   1 2 2 2 4 4 4
				//
				// 4. the leader will now probe the returning follower at index
				//    7, the rejection points it at the end of the follower's log
				//    which is at a higher log term than the actually committed
				//    log.
				nextProbeIdx, _ = r.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == tracker.StateReplicate {
					r.becomeProbe(pr)
				}
				r.maybeSendAppend(m.From)
			}
		} else {
			// We want to update our tracking if the response updates our
			// matched index or if the response can move a probing peer back
			// into StateReplicate (see heartbeat_rep_recovers_from_probing.txt
			// for an example of the latter case).
			// NB: the same does not make sense for StateSnapshot - if `m.Index`
			// equals pr.Match we know we don't m.Index+1 in our log, so moving
			// back to replicating state is not useful; besides pr.PendingSnapshot
			// would prevent it.
			if pr.MaybeUpdate(m.Index) || (pr.Match == m.Index && pr.State == tracker.StateProbe) {
				switch {
				case pr.State == tracker.StateProbe:
					r.becomeReplicate(pr)
				case pr.State == tracker.StateSnapshot && pr.Match+1 >= r.raftLog.firstIndex():
					// Note that we don't take into account PendingSnapshot to
					// enter this branch. No matter at which index a snapshot
					// was actually applied, as long as this allows catching up
					// the follower from the log, we will accept it. This gives
					// systems more flexibility in how they implement snapshots;
					// see the comments on PendingSnapshot.
					r.logger.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					// Transition back to replicating state via probing state
					// (which takes the snapshot into account). If we didn't
					// move to replicating state, that would only happen with
					// the next round of appends (but there may not be a next
					// round for a while, exposing an inconsistent RaftStatus).
					r.becomeProbe(pr)
					r.becomeReplicate(pr)
				case pr.State == tracker.StateReplicate:
					pr.Inflights.FreeLE(m.Index)
				}

				if r.maybeCommit() {
					r.bcastAppend()
				}
				// We've updated flow control information above, which may allow us to
				// send multiple (size-limited) in-flight messages at once (such as when
				// transitioning from probe to replicate, or when freeTo() covers
				// multiple messages). Send as many messages as we can.
				if r.id != m.From {
					for r.maybeSendAppend(m.From) {
					}
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.transferLeader(m.From)
				}
			}
		}

	case pb.MsgFortifyLeaderResp:
		r.handleFortifyResp(m)
		// We do the same as we do when receiving a MsgHeartbeatResp.
		// NB: We ignore self-addressed messages as we don't send MsgApp to
		// ourselves.
		if m.From != r.id {
			pr.RecentActive = true
			pr.MsgAppProbesPaused = false
			r.maybeSendAppend(m.From)
		}

	case pb.MsgHeartbeatResp:
		pr.RecentActive = true
		pr.MsgAppProbesPaused = false
		r.maybeSendAppend(m.From)

	case pb.MsgSnapStatus:
		if pr.State != tracker.StateSnapshot {
			return nil
		}
		if !m.Reject {
			r.becomeProbe(pr)
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			// NB: the order here matters or we'll be probing erroneously from
			// the snapshot index, but the snapshot never applied.
			pr.PendingSnapshot = 0
			r.becomeProbe(pr)
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the MsgAppResp from the remote node before sending
		// out the next MsgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		pr.MsgAppProbesPaused = true
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == tracker.StateReplicate {
			r.becomeProbe(pr)
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return nil
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset
		// r.electionElapsed. If the transfer target is not caught up on its log by
		// then, the transfer is aborted and the leader can resume normal operation.
		// See raft.abortLeaderTransfer.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
			r.transferLeader(leadTransferee)
		} else {
			// If the transfer target is not initially caught up on its log, we don't
			// send it a MsgTimeoutNow immediately. Instead, we eagerly try to catch
			// it up on its log so that it will be able to win the election when it
			// campaigns (recall that a candidate can only win an election if its log
			// is up-to-date). If we are able to successfully catch it up in time,
			// before the electionElapsed timeout fires, we call raft.transferLeader
			// in response to receiving the final MsgAppResp.
			pr.MsgAppProbesPaused = false
			r.maybeSendAppend(leadTransferee)
		}
	}
	return nil
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func stepCandidate(r *raft, m pb.Message) error {
	if IsMsgFromLeader(m.Type) && m.Type != pb.MsgDeFortifyLeader {
		// If this is a message from a leader of r.Term, transition to a follower
		// with the sender of the message as the leader, then process the message.
		// One exception is MsgDeFortifyLeader where it doesn't mean that there is
		// currently an active leader for this term.
		assertTrue(m.Term == r.Term, "message term should equal current term")
		r.becomeFollower(m.Term, m.From)
		return r.step(r, m) // stepFollower
	}
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == pb.StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MsgSnap:
		// TODO(nvanbenschoten): we can't consider MsgSnap to be from the leader of
		// Message.Term until we address #127348 and #127349.
		r.becomeFollower(m.Term, None)
		r.handleSnapshot(m)
	case myVoteRespType:
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
		switch res {
		case quorum.VoteWon:
			if r.state == pb.StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				r.becomeLeader()
				r.bcastFortify()
				r.bcastAppend()
			}
		case quorum.VoteLost:
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			r.becomeFollower(r.Term, None)
		}
	}
	return nil
}

func stepFollower(r *raft, m pb.Message) error {
	if IsMsgFromLeader(m.Type) {
		r.setLead(m.From)
		if m.Type != pb.MsgDeFortifyLeader {
			// If we receive any message from the leader except a MsgDeFortifyLeader,
			// we know that the leader is still alive and still acting as the leader,
			// so reset the election timer.
			r.electionElapsed = 0
		}
	}
	switch m.Type {
	case pb.MsgProp:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return ErrProposalDropped
		} else if r.lead == r.id {
			r.logger.Infof("%x not forwarding to itself at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgApp:
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.handleSnapshot(m)
	case pb.MsgFortifyLeader:
		r.handleFortify(m)
	case pb.MsgDeFortifyLeader:
		r.handleDeFortify(m)
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		} else if r.lead == r.id {
			r.logger.Infof("%x is itself the leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgForgetLeader:
		// TODO(nvanbenschoten): remove MsgForgetLeader once the sole caller in
		// Replica.maybeForgetLeaderOnVoteRequestLocked is removed. This will
		// accompany the removal of epoch-based leases.
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping forget leader msg", r.id, r.Term)
			return nil
		}
		if r.supportingFortifiedLeader() && r.lead != m.From {
			r.logger.Infof("%x [term %d] ignored MsgForgetLeader from %x due to leader fortification", r.id, r.Term, m.From)
			return nil
		}
		r.logger.Infof("%x forgetting leader %x at term %d", r.id, r.lead, r.Term)
		r.resetLead()
	case pb.MsgTimeoutNow:
		// TODO(nvanbenschoten): we will eventually want some kind of logic like
		// this. However, even this may not be enough, because we're calling a
		// campaignTransfer election, which bypasses leader fortification checks. It
		// may never be safe for MsgTimeoutNow to come from anyone but the leader.
		// We need to think about this more.
		//
		// if r.supportingFortifiedLeader() && r.lead != m.From {
		//	r.logger.Infof("%x [term %d] ignored MsgTimeoutNow from %x due to leader fortification", r.id, r.Term, m.From)
		//	return nil
		// }
		r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership", r.id, r.Term, m.From)
		// Leadership transfers never use pre-vote even if r.preVote is true; we
		// know we are not recovering from a partition so there is no need for the
		// extra round trip.
		// TODO(nvanbenschoten): Once the TODO above is addressed, and assuming its
		// handled by ensuring MsgTimeoutNow only comes from the leader, we should
		// be able to replace this leadEpoch assignment with a call to deFortify.
		// Currently, it may panic because only the leader should be able to
		// de-fortify without bumping the term.
		r.resetLeadEpoch()
		r.hup(campaignTransfer)
	}
	return nil
}

// checkQuorumActive ensures that the leader is supported by a quorum. If not,
// the leader steps down to a follower.
func (r *raft) checkQuorumActive() {
	assertTrue(r.state == pb.StateLeader, "checkQuorum in a non-leader state")

	quorumActiveByHeartbeats := r.trk.QuorumActive()
	quorumActiveByFortification := r.fortificationTracker.QuorumActive()
	if !quorumActiveByHeartbeats {
		r.logger.Debugf(
			"%x has not received messages from a quorum of peers in the last election timeout", r.id,
		)
	}
	if !quorumActiveByFortification {
		r.logger.Debugf("%x does not have store liveness support from a quorum of peers", r.id)
	}
	if !quorumActiveByHeartbeats && !quorumActiveByFortification {
		r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
		r.becomeFollower(r.Term, None)
	}
	// Mark everyone (but ourselves) as inactive in preparation for the next
	// CheckQuorum.
	r.trk.Visit(func(id pb.PeerID, pr *tracker.Progress) {
		if id != r.id {
			pr.RecentActive = false
		}
	})
}

// logSliceFromMsgApp extracts the appended LogSlice from a MsgApp message.
func logSliceFromMsgApp(m *pb.Message) LogSlice {
	// TODO(pav-kv): consider also validating the LogSlice here.
	return LogSlice{
		term:    m.Term,
		prev:    entryID{term: m.LogTerm, index: m.Index},
		entries: m.Entries,
	}
}

func (r *raft) handleAppendEntries(m pb.Message) {
	r.checkMatch(m.Match)

	// TODO(pav-kv): construct LogSlice up the stack next to receiving the
	// message, and validate it before taking any action (e.g. bumping term).
	a := logSliceFromMsgApp(&m)
	if err := a.valid(); err != nil {
		// TODO(pav-kv): add a special kind of logger.Errorf that panics in tests,
		// but logs an error in prod. We want to eliminate all such errors in tests.
		r.logger.Errorf("%x received an invalid MsgApp: %v", r.id, err)
		return
	}

	if a.prev.index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed,
			Commit: r.raftLog.committed})
		return
	}
	if r.raftLog.maybeAppend(a) {
		// TODO(pav-kv): make it possible to commit even if the append did not
		// succeed or is stale. If accTerm >= m.Term, then our log contains all
		// committed entries at m.Term (by raft invariants), so it is safe to bump
		// the commit index even if the MsgApp is stale.
		lastIndex := a.lastIndex()
		r.raftLog.commitTo(LogMark{Term: m.Term, Index: min(m.Commit, lastIndex)})
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: lastIndex,
			Commit: r.raftLog.committed})
		return
	}
	r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
		r.id, r.raftLog.zeroTermOnOutOfBounds(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)

	// Our log does not match the leader's at index m.Index. Return a hint to the
	// leader - a guess on the maximal (index, term) at which the logs match. Do
	// this by searching through the follower's log for the maximum (index, term)
	// pair with a term <= the MsgApp's LogTerm and an index <= the MsgApp's
	// Index. This can help skip all indexes in the follower's uncommitted tail
	// with terms greater than the MsgApp's LogTerm.
	//
	// See the other caller for findConflictByTerm (in stepLeader) for a much more
	// detailed explanation of this mechanism.

	// NB: m.Index >= raftLog.committed by now (see the early return above), and
	// raftLog.lastIndex() >= raftLog.committed by invariant, so min of the two is
	// also >= raftLog.committed. Hence, the findConflictByTerm argument is within
	// the valid interval, which then will return a valid (index, term) pair with
	// a non-zero term (unless the log is empty). However, it is safe to send a zero
	// LogTerm in this response in any case, so we don't verify it here.
	hintIndex := min(m.Index, r.raftLog.lastIndex())
	hintIndex, hintTerm := r.raftLog.findConflictByTerm(hintIndex, m.LogTerm)
	r.send(pb.Message{
		To:    m.From,
		Type:  pb.MsgAppResp,
		Index: m.Index,
		// This helps the leader track the follower's commit index. This flow is
		// independent from accepted/rejected log appends.
		Commit:     r.raftLog.committed,
		Reject:     true,
		RejectHint: hintIndex,
		LogTerm:    hintTerm,
	})
}

// checkMatch ensures that the follower's log size does not contradict to the
// leader's idea where it matches.
func (r *raft) checkMatch(match uint64) {
	// TODO(pav-kv): lastIndex() might be not yet durable. Make this check
	// stronger by comparing `match` with the last durable index.
	//
	// TODO(pav-kv): make this check stronger when the raftLog stores the last
	// accepted term. If `match` is non-zero, this follower's log last accepted
	// term must equal the leader term, and have entries up to `match` durable.
	if last := r.raftLog.lastIndex(); last < match {
		r.logger.Panicf("match(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", match, last)
	}
}

func (r *raft) handleHeartbeat(m pb.Message) {
	r.checkMatch(m.Match)
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp})
}

func (r *raft) handleSnapshot(m pb.Message) {
	// MsgSnap messages should always carry a non-nil Snapshot, but err on the
	// side of safety and treat a nil Snapshot as a zero-valued Snapshot.
	s := snapshot{term: m.Term}
	if m.Snapshot != nil {
		s.snap = *m.Snapshot
	}
	if err := s.valid(); err != nil {
		// TODO(pav-kv): add a special kind of logger.Errorf that panics in tests,
		// but logs an error in prod. We want to eliminate all such errors in tests.
		r.logger.Errorf("%x received an invalid MsgSnap: %v", r.id, err)
		return
	}

	id := s.lastEntryID()
	if r.restore(s) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, id.index, id.term)

		// To send MsgAppResp to any leader, we must be sure that our log is
		// consistent with that leader's log.
		//
		// After restore(s), our log ends at the entryID of this snapshot.
		// The snapshot came from a node at m.Term (typically the leader).
		// Everything in this snapshot has been committed during m.Term or earlier,
		// and will be present in all future term logs. It is thus safe to send
		// MsgAppResp to any leader at term >= m.Term.
		//
		// From section 5.4 of the Raft paper:
		// if a log entry is committed in a given term, then that entry will be
		// present in the logs of the leaders for all higher-numbered terms.
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex(),
			Commit: r.raftLog.committed})

		// A leadership change may have happened while the snapshot was in flight.
		// Therefore we need to send the MsgAppResp to the new leader as well.
		//
		// If we don't send response to the new leader, the new leader will not
		// know that we have committed up to the snapshot index. This will cause
		// delay for the new leader to transfer this follower to stateReplicate.
		// (Since the new leader may send its own snapshot to this follower and
		// wait for the MsgAppResp of that snapshot).
		// Which may ultimately cause unwanted commit delay for client.
		//
		// Sending this response to the new leader allows the new leader to know
		// this follower is caught up ASAP.
		//
		// TODO(pav-kv): consider if we can only send to r.lead.
		if r.lead != None && r.lead != m.From {
			r.send(pb.Message{To: r.lead, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex(),
				Commit: r.raftLog.committed})
		}
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, id.index, id.term)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed,
			Commit: r.raftLog.committed})
	}
}

func (r *raft) handleFortify(m pb.Message) {
	assertTrue(r.state == pb.StateFollower, "leaders should locally fortify without sending a message")
	assertTrue(r.lead == m.From, "only the leader should send fortification requests")

	epoch, live := r.storeLiveness.SupportFor(r.lead)
	if !live {
		// The leader isn't supported by this peer in its liveness fabric. Reject
		// the fortification request.
		r.send(pb.Message{
			To:     m.From,
			Type:   pb.MsgFortifyLeaderResp,
			Reject: true,
		})
		return
	}
	r.setLeadEpoch(epoch)
	r.send(pb.Message{
		To:        m.From,
		Type:      pb.MsgFortifyLeaderResp,
		LeadEpoch: epoch,
	})
}

func (r *raft) handleFortifyResp(m pb.Message) {
	assertTrue(r.state == pb.StateLeader, "only leaders should be handling fortification responses")
	if m.Reject {
		// Couldn't successfully fortify the follower. Typically, this happens when
		// the follower isn't supporting the leader's store in StoreLiveness or the
		// follower is down. We'll try to fortify the follower again later in
		// tickHeartbeat.
		r.metrics.RejectedFortificationResponses.Inc(1)
		return
	}

	r.metrics.AcceptedFortificationResponses.Inc(1)
	r.fortificationTracker.RecordFortification(m.From, m.LeadEpoch)
}

func (r *raft) handleDeFortify(m pb.Message) {
	assertTrue(r.state != pb.StateLeader, "leaders should locally de-fortify without sending a message")
	assertTrue(r.lead == None || r.lead == m.From,
		"only the leader should send de-fortification requests",
	)

	if r.leadEpoch == 0 {
		r.logger.Debugf("%d is not fortifying %d; de-fortification is a no-op", r.id, m.From)
	}

	r.deFortify(m.From, m.Term)
}

// deFortify revokes previously provided fortification to a leader.
//
// The from argument corresponds to the ID of the peer initiating (directly[1]
// or indirectly[2]) the de-fortification. The term argument corresponds to the
// term known to the peer[3] initiating de-fortification.
//
// [1] For example, if a peer realizes it's no longer supporting a previously
// fortified leader or if a previously fortified leader steps down and
// broadcasts MsgDeFortifyLeader to the raft group.
//
// [2] For example, if a new leader is elected without a vote from this peer
// (who continues to fortify the previous leader). The peer then learns about
// the previous leadership term ending when the new leader contacts it.
//
// [3] Note that this may not be the term being de-fortified. The term being
// de-fortified is r.Term, which is the term of the peer being de-fortified.
func (r *raft) deFortify(from pb.PeerID, term uint64) {
	if r.leadEpoch == 0 {
		return // we're not currently fortified, so de-fortification is a no-op
	}

	assertTrue(
		// We were fortified at a lower term that has since advanced...
		term > r.Term ||
			// ...OR the current term is being explicitly de-fortified by the leader...
			(term == r.Term && from == r.lead) ||
			// ...OR we've unilaterally decided to de-fortify because we are no longer
			// supporting the fortified leader (in StoreLiveness).
			(term == r.Term && from == r.id && !r.supportingFortifiedLeader()),
		"can only defortify at current term if told by the leader or if fortification has expired",
	)

	r.resetLeadEpoch()

	// The peer is not fortifying the leader anymore. As a result:
	// 1. We don't want to wait out an entire election timeout before
	//    campaigning.
	// 2. But we do want to take advantage of randomized election timeouts built
	//    into raft to prevent hung elections.
	// We achieve both of these goals by "forwarding" electionElapsed to begin
	// at r.electionTimeout. Also see atRandomizedElectionTimeout.
	r.logger.Debugf(
		"%d setting election elapsed to start from %d ticks after store liveness support expired",
		r.id, r.electionTimeout,
	)
	r.electionElapsed = r.electionTimeout
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine. If this method returns false, the snapshot was
// ignored, either because it was obsolete or because of an error.
func (r *raft) restore(s snapshot) bool {
	id := s.lastEntryID()
	if id.index <= r.raftLog.committed {
		return false
	}
	if r.state != pb.StateFollower {
		// This is defense-in-depth: if the leader somehow ended up applying a
		// snapshot, it could move into a new term without moving into a
		// follower state. This should never fire, but if it does, we panic.
		//
		// At the time of writing, the instance is guaranteed to be in follower
		// state when this method is called.
		r.logger.Panicf("%x attempted to restore snapshot in state %s", r.id, r.state)
		return false
	}

	// More defense-in-depth: throw away snapshot if recipient is not in the
	// config. This shouldn't ever happen (at the time of writing) but lots of
	// code here and there assumes that r.id is in the progress tracker.
	found := false
	cs := s.snap.Metadata.ConfState

	for _, set := range [][]pb.PeerID{
		cs.Voters,
		cs.Learners,
		cs.VotersOutgoing,
		// `LearnersNext` doesn't need to be checked. According to the rules, if a peer in
		// `LearnersNext`, it has to be in `VotersOutgoing`.
	} {
		for _, id := range set {
			if id == r.id {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		r.logger.Warningf(
			"%x attempted to restore snapshot but it is not in the ConfState %v; should never happen",
			r.id, cs,
		)
		return false
	}

	// Now go ahead and actually restore.

	if r.raftLog.matchTerm(id) {
		// TODO(pav-kv): can print %+v of the id, but it will change the format.
		last := r.raftLog.lastEntryID()
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, last.index, last.term, id.index, id.term)
		r.raftLog.commitTo(s.mark())
		return false
	}

	if !r.raftLog.restore(s) {
		r.logger.Errorf("%x unable to restore snapshot [index: %d, term: %d]", r.id, id.index, id.term)
		return false
	}

	cfg, progressMap, err := confchange.Restore(confchange.Changer{
		// Reset the configuration and add the (potentially updated) peers in anew.
		Config:           quorum.MakeEmptyConfig(),
		ProgressMap:      tracker.MakeEmptyProgressMap(), // empty ProgressMap to go with our empty config
		MaxInflight:      r.maxInflight,
		MaxInflightBytes: r.maxInflightBytes,
		LastIndex:        r.raftLog.lastIndex(),
	}, cs)

	if err != nil {
		// This should never happen. Either there's a bug in our config change
		// handling or the client corrupted the conf change.
		panic(fmt.Sprintf("unable to restore config %+v: %s", cs, err))
	}

	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, progressMap))

	last := r.raftLog.lastEntryID()
	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] restored snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, last.index, last.term, id.index, id.term)
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	pr := r.trk.Progress(r.id)
	return pr != nil && !pr.IsLearner && !r.raftLog.hasNextOrInProgressSnapshot()
}

func (r *raft) applyConfChange(cc pb.ConfChangeV2) pb.ConfState {
	cfg, progressMap, err := func() (quorum.Config, tracker.ProgressMap, error) {
		changer := confchange.Changer{
			Config:           r.config,
			ProgressMap:      r.trk.MoveProgressMap(),
			MaxInflight:      r.maxInflight,
			MaxInflightBytes: r.maxInflightBytes,
			LastIndex:        r.raftLog.lastIndex(),
		}
		if cc.LeaveJoint() {
			return changer.LeaveJoint()
		} else if autoLeave, ok := cc.EnterJoint(); ok {
			return changer.EnterJoint(autoLeave, cc.Changes...)
		}
		return changer.Simple(cc.Changes...)
	}()

	if err != nil {
		// TODO(tbg): return the error to the caller.
		panic(err)
	}

	return r.switchToConfig(cfg, progressMap)
}

// switchToConfig reconfigures this node to use the provided configuration. It
// updates the in-memory state and, when necessary, carries out additional
// actions such as reacting to the removal of nodes or changed quorum
// requirements.
//
// The inputs usually result from restoring a ConfState or applying a ConfChange.
func (r *raft) switchToConfig(cfg quorum.Config, progressMap tracker.ProgressMap) pb.ConfState {
	r.config = cfg
	r.trk = tracker.MakeProgressTracker(&r.config, progressMap)

	r.logger.Infof("%x switched to configuration %s", r.id, r.config)
	cs := r.config.ConfState()
	pr := r.trk.Progress(r.id)

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
	r.isLearner = pr != nil && pr.IsLearner

	// The remaining steps only make sense if this node is the leader and there
	// are other nodes.
	if r.state != pb.StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	if pr == nil {
		// This node is leader and was removed. This should not be possible, as we
		// do not allow voters to be removed directly without first being demoted
		// to a learner, and only voters may serve as leader. Direct removal of the
		// raft leader is unsafe for at least two reasons:
		// 1. the leader (or any voter) may be needed to vote for a candidate who
		//    has not yet applied the configuration change. This is a liveness issue
		//    if the leader/voter is immediately removed without stepping down to a
		//    learner first and waiting for a second configuration change to
		//    succeed.
		//    For details, see: https://github.com/cockroachdb/cockroach/pull/42251.
		// 2. the leader may have fortified its leadership term, binding the
		//    liveness of the leader replica to the leader's store's store liveness
		//    heartbeats. Removal of the leader replica from a store while that
		//    store continues to heartbeat in the store liveness fabric will lead to
		//    the leader disappearing without any other replica deciding that the
		//    leader is gone and stepping up to campaign.
		r.logger.Panicf("%x leader removed from configuration %s", r.id, r.config)
	}

	// Config changes might cause the LeadSupportUntil to change, we need to
	// recalculate it here.
	r.fortificationTracker.ComputeLeadSupportUntil(r.state)

	if r.isLearner {
		// This node is leader and was demoted, step down.
		//
		// TODO(tbg): ask follower with largest Match to TimeoutNow (to avoid
		// interruption). This might still drop some proposals but it's better than
		// nothing.
		//
		// A learner can't campaign or participate in elections, and in order for a
		// learner to get promoted to a voter, it needs a new leader to get elected
		// and propose that change. Therefore, it should be safe at this point to
		// defortify and forget that we were the leader at this term and step down.
		r.deFortify(r.id, r.Term)
		r.becomeFollower(r.Term, None)
		return cs
	}

	r.maybeCommit()
	// If the configuration change means that there are new followers, fortify
	// them immediately, without waiting for the next heartbeatTimeout to fire.
	// bcastFortify is a no-op for peers that are already fortified.
	r.bcastFortify()
	// If the configuration change means that more entries are committed now,
	// broadcast/append to everyone in the updated config.
	//
	// Otherwise, still probe the newly added replicas; there's no reason to let
	// them wait out a heartbeat interval (or the next incoming proposal).
	r.bcastAppend()

	// If the leadTransferee was removed or demoted, abort the leadership transfer.
	if _, tOK := r.config.Voters.IDs()[r.leadTransferee]; !tOK && r.leadTransferee != 0 {
		r.abortLeaderTransfer()
	}

	return cs
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.setTerm(state.Term)
	r.setVote(state.Vote)
	if state.LeadEpoch != 0 {
		// A non-zero lead epoch indicates that the leader fortified its term.
		// Fortification promises should hold true across restarts, so we need to
		// restore both the lead and the lead epoch.
		//
		// In cases where the leader wasn't fortified prior to the restart, we
		// eschew loading the leader known to this peer before shutdown. This
		// maintains parity with how raft restarts worked before the fortification
		// protocol was introduced. While it isn't incorrect to load the leader, it
		// does trip the inHeartbeatLease condition without considerable care.
		// Tripping the inHeartbeatLease condition can delay leader elections by 2s.
		// This is known to be a source of regressions, which become meaningful when
		// the 2s delay stacks by a O(ranges) factor. Epoch based leases which are
		// quiesced before a restart are particularly vulnerable to such
		// regressions. Not loading the leader if the leader wasn't fortified is a
		// way by which we avoid this known and possibly other unknown regressions.
		r.setLead(state.Lead)
		r.setLeadEpoch(state.LeadEpoch)
	}
}

// atRandomizedElectionTimeout returns true if r.electionElapsed modulo the
// r.randomizedElectionTimeout is equal to 0. This means that at every
// r.randomizedElectionTimeout period, this method will return true once.
func (r *raft) atRandomizedElectionTimeout() bool {
	return r.electionElapsed != 0 && r.electionElapsed%r.randomizedElectionTimeout == 0
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Int63n(r.electionTimeout)
}

func (r *raft) transferLeader(to pb.PeerID) {
	assertTrue(r.state == pb.StateLeader, "only the leader can transfer leadership")
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
	// When a leader transfers leadership to another replica, it instructs the
	// replica to campaign without performing the campaign checks. Therefore, it
	// should be safe to defortify and forget the we were the leader at this term
	// when stepping down.
	r.deFortify(r.id, r.Term)
	r.becomeFollower(r.Term, None)
}

func (r *raft) abortLeaderTransfer() {
	// TODO(arul): we currently call this method regardless of whether we are the
	// leader or not and regardless of whether there is an in-progress leadership
	// transfer. We should consider limiting the cases where this can be called
	// and adding the appropriate preconditions as assertions.
	r.leadTransferee = None
}

// increaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
//
// Empty payloads are never refused. This is used both for appending an empty
// entry at a new leader's term, as well as leaving a joint configuration.
func (r *raft) increaseUncommittedSize(ents []pb.Entry) bool {
	s := payloadsSize(ents)
	if r.uncommittedSize > 0 && s > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		// Note the added requirement s>0 which is used to make sure that
		// appending single empty entries to the log always succeeds, used both
		// for replicating a new leader's initial empty entry, and for
		// auto-leaving joint configurations.
		return false
	}
	r.uncommittedSize += s
	return true
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (r *raft) reduceUncommittedSize(s entryPayloadSize) {
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}

// markFortifyingFollowersAsRecentlyActive iterates over all the followers, and
// mark them as recently active if they are supporting the leader.
func (r *raft) markFortifyingFollowersAsRecentlyActive() {
	r.trk.Visit(func(id pb.PeerID, pr *tracker.Progress) {
		if pr.RecentActive {
			return // return early as it's already marked as recently active
		}

		if isFortified, _ := r.fortificationTracker.IsFortifiedBy(id); isFortified {
			pr.RecentActive = true
		}
	})
}

func (r *raft) testingStepDown() error {
	if r.lead != r.id {
		return errors.New("cannot step down if not the leader")
	}
	// De-fortify ourselves before stepping down to forget the lead epoch.
	// Otherwise, the leadEpoch may be set when the lead field isn't.
	r.deFortify(r.id, r.Term)
	r.becomeFollower(r.Term, None)
	return nil
}

func (r *raft) testingSendDeFortify(to pb.PeerID) error {
	r.sendDeFortify(to)
	return nil
}
