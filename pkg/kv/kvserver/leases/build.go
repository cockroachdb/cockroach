// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package leases

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Settings is the set of settings for the leasing subsystem, used when building
// a new lease object.
type Settings struct {
	// UseExpirationLeases controls whether this range should be using an
	// expiration-based lease.
	UseExpirationLeases bool
	// TransferExpirationLeases controls whether we transfer expiration-based
	// leases that are later upgraded to epoch-based ones or whether we transfer
	// epoch-based leases directly.
	TransferExpirationLeases bool
	// RejectLeaseOnLeaderUnknown controls whether a replica that does not know
	// the current raft leader rejects a lease request.
	RejectLeaseOnLeaderUnknown bool
	// DisableAboveRaftLeaseTransferSafetyChecks, if set, disables the above-raft
	// lease transfer safety checks (that verify that we don't transfer leases to
	// followers that need a snapshot, etc). The proposal-time checks are not
	// affected by this knob.
	DisableAboveRaftLeaseTransferSafetyChecks bool
	// AllowLeaseProposalWhenNotLeader, if set, allows lease request proposals
	// even when the replica inserting that proposal is not the Raft leader. This
	// can be used in tests to allow a replica to acquire a lease without first
	// moving the Raft leadership to it (e.g. it allows tests to expire leases by
	// stopping the old leaseholder's liveness heartbeats and then expect other
	// replicas to take the lease without worrying about Raft).
	AllowLeaseProposalWhenNotLeader bool
	// ExpToEpochEquiv indicates whether an expiration-based lease can be
	// considered equivalent to an epoch-based lease during a promotion from
	// expiration-based to epoch-based. It is used for mixed-version
	// compatibility.
	ExpToEpochEquiv bool
	// MinExpirationSupported indicates whether the cluster version supports the
	// minimum expiration field in the lease object. It is used for mixed-version
	// compatibility.
	MinExpirationSupported bool
	// RangeLeaseDuration specifies the range lease duration.
	RangeLeaseDuration time.Duration
}

// NodeLiveness is a read-only interface to the node liveness subsystem.
type NodeLiveness interface {
	GetLiveness(roachpb.NodeID) (liveness.Record, bool)
}

// NodeLivenessManipulation contains a set of instructions for manipulating node
// liveness records during the lease acquisition process. These actions must be
// taken before the next lease can be requested.
type NodeLivenessManipulation struct {
	// Heartbeat, if not nil, is the liveness record of the local node which must
	// be heartbeat before the next lease can be requested. If this heartbeat
	// fails, the lease request must be rejected.
	Heartbeat *livenesspb.Liveness
	// HeartbeatMinExpiration, if set, is the minimum expiration time that the
	// liveness record must be heartbeat to. This is used to ensure that the
	// expiration of the liveness record does not regress during a promotion from
	// an expiration-based lease to an epoch-based lease.
	HeartbeatMinExpiration hlc.Timestamp
	// Increment, if not nil, is the liveness record of the previous leaseholder
	// which must have its epoch incremented before the next lease can be
	// requested. If this increment fails, the lease request must be rejected.
	Increment *livenesspb.Liveness
}

// BuildInput is the set of input parameters for the lease acquisition process.
type BuildInput struct {
	// Information about the local replica.
	LocalStoreID       roachpb.StoreID
	LocalReplicaID     roachpb.ReplicaID
	Desc               *roachpb.RangeDescriptor
	Now                hlc.ClockTimestamp
	MinLeaseProposedTS hlc.ClockTimestamp

	// Information about raft.
	RaftStatus     *raft.Status
	RaftFirstIndex kvpb.RaftIndex

	// Information about the previous lease.
	PrevLease roachpb.Lease
	// PrevLeaseNodeLiveness is set iff PrevLease is an epoch-based lease.
	PrevLeaseNodeLiveness livenesspb.Liveness
	// PrevLeaseExpired is set iff the previous lease has expired at Now.
	PrevLeaseExpired bool

	// Information about the (requested) next lease.
	NextLeaseHolder roachpb.ReplicaDescriptor

	// When set to true, BypassSafetyChecks configures lease transfers to skip
	// safety checks that ensure that the transfer target is known to be
	// (according to the outgoing leaseholder) alive and sufficiently caught up on
	// its log. This option should be used sparingly — typically only by outgoing
	// leaseholders who both have some other reason to believe that the target is
	// alive and caught up on its log (e.g. they just sent it a snapshot) and also
	// can't tolerate rejected lease transfers.
	BypassSafetyChecks bool
}

// PrevLocal returns whether the previous lease was held by the local store.
func (i BuildInput) PrevLocal() bool { return i.PrevLease.Replica.StoreID == i.LocalStoreID }

// NextLocal returns whether the next lease will be held by the local store.
func (i BuildInput) NextLocal() bool { return i.NextLeaseHolder.StoreID == i.LocalStoreID }

// Acquisition returns whether the lease request is an acquisition.
func (i BuildInput) Acquisition() bool { return !i.PrevLocal() && i.NextLocal() }

// Extension returns whether the lease request is an extension.
func (i BuildInput) Extension() bool { return i.PrevLocal() && i.NextLocal() }

// Transfer returns whether the lease request is a transfer.
func (i BuildInput) Transfer() bool { return i.PrevLocal() && !i.NextLocal() }

// Remote returns whether the lease request is a remote transfer.
func (i BuildInput) Remote() bool { return !i.PrevLocal() && !i.NextLocal() }

// PrevLeaseExpiration returns the expiration time of the previous lease.
func (i BuildInput) PrevLeaseExpiration() hlc.Timestamp {
	return kvserverpb.LeaseStatus{
		Lease:    i.PrevLease,
		Liveness: i.PrevLeaseNodeLiveness,
	}.Expiration()
}

func (i BuildInput) validate() error {
	if i.NextLeaseHolder == (roachpb.ReplicaDescriptor{}) {
		return errors.AssertionFailedf("no lease target provided")
	}
	if i.Now.IsEmpty() {
		return errors.AssertionFailedf("no clock timestamp provided")
	}
	if i.Remote() {
		return errors.AssertionFailedf("cannot acquire/extend lease for remote "+
			"replica: %v -> %v", i.PrevLease, i.NextLeaseHolder)
	}
	if err := i.validatePrevLeaseNodeLiveness(); err != nil {
		return err
	}
	if err := i.validatePrevLeaseExpired(); err != nil {
		return err
	}
	if i.Acquisition() && !i.PrevLeaseExpired {
		// If this is a non-cooperative lease change (i.e. an acquisition), it is up
		// to us to ensure that Lease.Start is greater than the end time of the
		// previous lease. This means that if BuildInput refers to an expired epoch
		// lease, we must increment the liveness epoch of the previous leaseholder
		// *using BuildInput.PrevLeaseNodeLiveness*, which we know to be expired *at
		// BuildInput.Now*, before we can propose this lease. If this increment
		// fails, we cannot propose this new lease (see handling of
		// ErrEpochAlreadyIncremented in requestLeaseAsync).
		//
		// Note that the request evaluation may decrease our proposed start time
		// if it decides that it is safe to do so (for example, this happens
		// when renewing an expiration-based lease), but it will never increase
		// it (and a start timestamp that is too low is unsafe because it
		// results in incorrect initialization of the timestamp cache on the new
		// leaseholder). For expiration-based leases, we have a safeguard during
		// evaluation - we simply check that the new lease starts after the old
		// lease ends and throw an error if now. But for epoch-based leases, we
		// don't have the benefit of such a safeguard during evaluation because
		// the expiration is indirectly stored in the referenced liveness record
		// and not in the lease itself. So for epoch-based leases, enforcing
		// this safety condition is truly up to us.
		return errors.AssertionFailedf("cannot acquire lease from another node "+
			"before it has expired: %v", i.PrevLease)
	}
	if !i.Transfer() && i.BypassSafetyChecks {
		return errors.AssertionFailedf("cannot bypass safety checks for lease acquisition/extension")
	}
	return nil
}

func (i BuildInput) validatePrevLeaseNodeLiveness() error {
	epochLease := i.PrevLease.Type() == roachpb.LeaseEpoch
	livenessSet := i.PrevLeaseNodeLiveness != livenesspb.Liveness{}
	if epochLease != livenessSet {
		return errors.AssertionFailedf("previous lease is epoch-based: %t, "+
			"but liveness is set: %t", epochLease, livenessSet)
	}
	return nil
}

func (i BuildInput) validatePrevLeaseExpired() error {
	expired := i.PrevLeaseExpiration().LessEq(i.Now.ToTimestamp())
	if i.PrevLeaseExpired != expired {
		return errors.AssertionFailedf("PrevLeaseExpired=%t, but computed %t "+
			"with PrevLease=%v and PrevLeaseNodeLiveness=%v",
			i.PrevLeaseExpired, expired, i.PrevLease, i.PrevLeaseNodeLiveness)
	}
	return nil
}

// toVerifyInput converts the BuildInput to a VerifyInput, which is used to
// verify the safety of lease requests. This is a non-lossy conversion, so the
// resulting VerifyInput should be fully populated, which is verified in
// TestInputToVerifyInput.
func (i BuildInput) toVerifyInput() VerifyInput {
	return VerifyInput{
		LocalStoreID:       i.LocalStoreID,
		LocalReplicaID:     i.LocalReplicaID,
		Desc:               i.Desc,
		RaftStatus:         i.RaftStatus,
		RaftFirstIndex:     i.RaftFirstIndex,
		PrevLease:          i.PrevLease,
		PrevLeaseExpired:   i.PrevLeaseExpired,
		NextLeaseHolder:    i.NextLeaseHolder,
		BypassSafetyChecks: i.BypassSafetyChecks,
	}
}

// Output is the set of outputs for the lease acquisition process.
type Output struct {
	NextLease                roachpb.Lease
	NodeLivenessManipulation NodeLivenessManipulation
}

func (o Output) validate(i BuildInput) error {
	for _, f := range leaseValidationFuncs {
		if err := f(i, o.NextLease); err != nil {
			return err
		}
	}
	return nil
}

// VerifyAndBuild checks the safety of a lease acquisition or transfer request.
// If the safety checks fail, it returns an error.
//
// If the safety checks pass, it constructs a new lease based on the input
// settings and parameters. The resulting output will contain the lease to be
// proposed and any necessary node liveness manipulations that must be performed
// before the lease can be requested.
func VerifyAndBuild(
	ctx context.Context, st Settings, nl NodeLiveness, i BuildInput,
) (Output, error) {
	if err := i.validate(); err != nil {
		return Output{}, err
	}
	if i.Transfer() && !st.DisableAboveRaftLeaseTransferSafetyChecks {
		// TODO(nvanbenschoten): support build-time verification for lease
		// acquisition, not just lease transfers. This currently breaks various
		// tests. See #118435.
		if err := Verify(ctx, st, i.toVerifyInput()); err != nil {
			return Output{}, err
		}
	}
	return build(st, nl, i)
}

func build(st Settings, nl NodeLiveness, i BuildInput) (Output, error) {
	// Construct the next lease.
	nextLease := roachpb.Lease{
		Replica:         leaseReplica(i),
		Start:           leaseStart(i),
		ProposedTS:      leaseProposedTS(i),
		AcquisitionType: leaseAcquisitionType(i),
	}

	// Configure fields that are specific to the lease type.
	nextLeaseType := leaseType(st, i)
	var nextLeaseLiveness *livenesspb.Liveness
	switch nextLeaseType {
	case roachpb.LeaseExpiration:
		nextLease.Expiration = leaseExpiration(st, i, nextLeaseType)
	case roachpb.LeaseEpoch:
		l, err := leaseEpoch(nl, i, nextLeaseType)
		if err != nil {
			return Output{}, &kvpb.LeaseRejectedError{
				Existing:  i.PrevLease,
				Requested: nextLease,
				Message:   err.Error(),
			}
		}
		nextLease.Epoch = l.Epoch
		nextLeaseLiveness = &l.Liveness

		nextLease.MinExpiration = leaseMinTimestamp(st, i, nextLeaseType)
	default:
		panic("unexpected")
	}

	// TODO(nvanbenschoten): remove in #124057 when clusterversion.MinSupported
	// is v24.1 or greater.
	nextLease.DeprecatedStartStasis = leaseDeprecatedStartStasis(i, nextLease.Expiration)

	// Finally, configure the lease sequence based on the previous lease and the
	// newly constructed lease.
	nextLease.Sequence = leaseSequence(st, i, nextLease)

	// Construct the output and determine whether any node liveness manipulation
	// is necessary before the lease can be requested.
	o := Output{NextLease: nextLease}
	o.NodeLivenessManipulation = nodeLivenessManipulation(st, i, nextLease, nextLeaseLiveness)

	// Validate the output.
	if err := o.validate(i); err != nil {
		return Output{}, err
	}
	return o, nil
}

func leaseType(st Settings, i BuildInput) roachpb.LeaseType {
	if st.UseExpirationLeases || (i.Transfer() && st.TransferExpirationLeases) {
		// In addition to ranges that should be using expiration-based leases
		// (typically the meta and liveness ranges), we also use them during lease
		// transfers for all other ranges. After acquiring these expiration based
		// leases, the leaseholders are expected to upgrade them to the more
		// efficient epoch-based ones. But by transferring an expiration-based
		// lease, we can limit the effect of an ill-advised lease transfer since the
		// incoming leaseholder needs to recognize itself as such within a few
		// seconds; if it doesn't (we accidentally sent the lease to a replica in
		// need of a snapshot or far behind on its log), the lease is up for grabs.
		// If we simply transferred epoch based leases, it's possible for the new
		// leaseholder that's delayed in applying the lease transfer to maintain its
		// lease (assuming the node it's on is able to heartbeat its liveness
		// record).
		return roachpb.LeaseExpiration
	}
	return roachpb.LeaseEpoch
}

func leaseReplica(i BuildInput) roachpb.ReplicaDescriptor {
	return i.NextLeaseHolder
}

func leaseProposedTS(i BuildInput) hlc.ClockTimestamp {
	return i.Now
}

func leaseStart(i BuildInput) hlc.ClockTimestamp {
	if i.Transfer() {
		// For lease transfers, we initially set the lease start time to now.
		// However, this will be adjusted during request evaluation to a clock
		// reading captured after the previous lease has been revoked (RevokeLease).
		return i.Now
	}

	start := i.Now
	if i.PrevLease.Replica.StoreID == 0 || i.Extension() {
		start.Backward(i.PrevLease.Start)
		// If the lease holder promised to not propose any commands below
		// minLeaseProposedTS, it must also not be allowed to extend a lease
		// before that timestamp. We make sure that when a node restarts, its
		// earlier in-flight commands (which are not tracked by the spanlatch
		// manager post restart) receive an error under the new lease by making
		// sure the sequence number of that lease is higher. This in turn is
		// achieved by forwarding its start time here, which makes it not
		// Equivalent() to the preceding lease for the same store.
		//
		// The same logic applies to lease transfers, which also bump the
		// minLeaseProposedTS in RevokeLease. By bumping the start time here, we
		// ensure that the sequence number of the new lease is higher than the
		// sequence number of the lease that was transferred away.
		//
		// Note also that leasePostApplyLocked makes sure to update the
		// timestamp cache in this case: even though the lease holder does not
		// change, the sequence number does and this triggers a low water mark
		// bump.
		//
		// The bug prevented with this is unlikely to occur in practice since
		// earlier commands usually apply before this lease will.
		if i.Extension() {
			start.Forward(i.MinLeaseProposedTS)
		}
	} else if i.PrevLease.Type() == roachpb.LeaseExpiration {
		// If the previous lease was an expiration-based lease, we can safely
		// backdate our start time all the way to its expiration time.
		start.BackwardWithTimestamp(i.PrevLease.Expiration.Next())
	}
	return start
}

func leaseAcquisitionType(i BuildInput) roachpb.LeaseAcquisitionType {
	if i.Transfer() {
		return roachpb.LeaseAcquisitionType_Transfer
	}
	return roachpb.LeaseAcquisitionType_Request
}

func leaseExpiration(st Settings, i BuildInput, nextType roachpb.LeaseType) *hlc.Timestamp {
	if nextType != roachpb.LeaseExpiration {
		panic("leaseExpiration called for non-expiration lease")
	}
	exp := i.Now.ToTimestamp().Add(int64(st.RangeLeaseDuration), 0)
	exp.Forward(i.PrevLeaseExpiration())
	return &exp
}

func leaseEpoch(
	nl NodeLiveness, i BuildInput, nextType roachpb.LeaseType,
) (liveness.Record, error) {
	if nextType != roachpb.LeaseEpoch {
		panic("leaseEpoch called for non-epoch lease")
	}
	l, ok := nl.GetLiveness(i.NextLeaseHolder.NodeID)
	if !ok || l.Epoch == 0 {
		return liveness.Record{}, errors.Wrapf(liveness.ErrRecordCacheMiss,
			"couldn't request lease for %+v", i.NextLeaseHolder)
	}
	return l, nil
}

func leaseMinTimestamp(st Settings, i BuildInput, nextType roachpb.LeaseType) hlc.Timestamp {
	if nextType != roachpb.LeaseEpoch {
		panic("leaseMinTimestamp called for non-epoch lease")
	}
	if !st.MinExpirationSupported {
		return hlc.Timestamp{}
	}
	// If we are promoting an expiration-based lease to an epoch-based lease, we
	// must make sure the expiration does not regress. Do so by assigning a
	// minimum expiration time to the new lease, which sets a lower bound for the
	// lease's expiration, independent of the expiration stored indirectly in the
	// liveness record.
	expPromo := i.Extension() && i.PrevLease.Type() == roachpb.LeaseExpiration
	if expPromo {
		return *i.PrevLease.Expiration
	}
	return hlc.Timestamp{}
}

func leaseDeprecatedStartStasis(i BuildInput, nextExpiration *hlc.Timestamp) *hlc.Timestamp {
	if i.Transfer() {
		// We don't set StartStasis for lease transfers. It's not clear why this was
		// ok in the past, but the field is unused now and only set for backwards
		// compatibility, so retain the behavior.
		return nil
	}
	return nextExpiration
}

func leaseSequence(st Settings, i BuildInput, nextLease roachpb.Lease) roachpb.LeaseSequence {
	// Return a sequence number for the next lease based on whether the lease is
	// equivalent to the one it's succeeding.
	if i.PrevLease.Equivalent(nextLease, st.ExpToEpochEquiv) {
		// If the proposed lease is equivalent to the previous lease, it is
		// given the same sequence number. This is subtle, but is important
		// to ensure that leases which are meant to be considered the same
		// lease for the purpose of matching leases during command execution
		// (see Lease.Equivalent) will be considered so. For example, an
		// extension to an expiration-based lease will result in a new lease
		// with the same sequence number.
		return i.PrevLease.Sequence
	} else {
		// We set the new lease sequence to one more than the previous lease
		// sequence. This is safe and will never result in repeated lease
		// sequences because the sequence check beneath Raft acts as an atomic
		// compare-and-swap of sorts. If two lease requests are proposed in
		// parallel, both with the same previous lease, only one will be
		// accepted and the other will get a LeaseRejectedError and need to
		// retry with a different sequence number. This is actually exactly what
		// the sequence number is used to enforce!
		return i.PrevLease.Sequence + 1
	}
}

func nodeLivenessManipulation(
	st Settings, i BuildInput, nextLease roachpb.Lease, nextLeaseLiveness *livenesspb.Liveness,
) NodeLivenessManipulation {
	// If we are promoting an expiration-based lease to an epoch-based lease, we
	// must make sure the expiration does not regress. We do this here because the
	// expiration is stored directly in the lease for expiration-based leases but
	// indirectly in liveness record for epoch-based leases. To ensure this, we
	// manually heartbeat our liveness record if necessary. This is expected to
	// work because the liveness record interval and the expiration-based lease
	// interval are the same.
	//
	// We only need to perform this check if the minimum expiration field is not
	// supported by the current cluster version. Otherwise, that field will be
	// used to enforce the minimum expiration time.
	//
	// TODO(nvanbenschoten): remove this logic when we no longer support clusters
	// that do not support the minimum expiration field.
	if !st.MinExpirationSupported {
		expToEpochPromo := i.Extension() &&
			i.PrevLease.Type() == roachpb.LeaseExpiration && nextLease.Type() == roachpb.LeaseEpoch
		if expToEpochPromo && nextLeaseLiveness.Expiration.ToTimestamp().Less(i.PrevLeaseExpiration()) {
			return NodeLivenessManipulation{
				Heartbeat:              nextLeaseLiveness,
				HeartbeatMinExpiration: i.PrevLeaseExpiration(),
			}
		}
	}

	// If we're replacing an expired epoch-based lease, we must increment the
	// epoch of the prior owner to invalidate its leases. If we were the owner,
	// then we instead heartbeat to become live.
	if i.PrevLease.Type() == roachpb.LeaseEpoch && i.PrevLeaseExpired {
		prevLeaseNodeLiveness := i.PrevLeaseNodeLiveness
		if i.Extension() {
			// If this replica is the previous and the next leaseholder, manually
			// heartbeat to become live.
			return NodeLivenessManipulation{Heartbeat: &prevLeaseNodeLiveness}
		} else if prevLeaseNodeLiveness.Epoch == i.PrevLease.Epoch {
			// If not owner, increment previous leaseholder's epoch to invalidate its
			// lease and prevent it from ever becoming valid again. We don't need to
			// grab a new start time for the lease after the increment because we use
			// the PrevLeaseNodeLiveness as a pre-condition for the node liveness
			// increment's conditional put. This means that if the increment succeeds,
			// we know that it was not extended beyond the start time we assigned to
			// the new lease.
			return NodeLivenessManipulation{Increment: &prevLeaseNodeLiveness}
		}
	}
	return NodeLivenessManipulation{}
}

var leaseValidationFuncs = []func(i BuildInput, nextLease roachpb.Lease) error{
	validateReplica,
	validateProposedTS,
	validateStart,
	validateExpiration,
	validateAcquisitionType,
	validateSequence,
}

func validateReplica(_ BuildInput, nextLease roachpb.Lease) error {
	return validateNonZero(nextLease.Replica, "replica")
}

func validateProposedTS(_ BuildInput, nextLease roachpb.Lease) error {
	return validateNonZero(nextLease.ProposedTS, "proposed timestamp")
}

func validateStart(i BuildInput, nextLease roachpb.Lease) error {
	if i.Now.Less(nextLease.Start) {
		return errors.AssertionFailedf("lease cannot start after now")
	}
	if i.Extension() || i.Transfer() {
		// If this is an extension or a transfer, the next lease's start time can
		// overlap with the previous lease's interval (prevLease.expiration >
		// nextLease.start is ok), but the next lease's start time cannot be before
		// the previous lease started (prevLease.start > nextLease.start is NOT ok).
		if nextLease.Start.Less(i.PrevLease.Start) {
			return errors.AssertionFailedf("extension/transfer cannot regress start timestamp")
		}
	} else /* i.Acquisition() */ {
		// If this is not an extension nor a transfer, the next lease's start time
		// cannot overlap with the previous lease interval (prevLease.expiration >
		// nextLease.start is NOT ok).
		if nextLease.Start.ToTimestamp().Less(i.PrevLeaseExpiration()) {
			return errors.AssertionFailedf("lease overlaps previous lease")
		}
	}
	return nil
}

func validateExpiration(_ BuildInput, nextLease roachpb.Lease) error {
	switch nextLease.Type() {
	case roachpb.LeaseExpiration:
		if nextLease.Expiration == nil {
			return errors.AssertionFailedf("expiration not assigned to expiration-based lease")
		}
		if nextLease.Expiration.LessEq(nextLease.Start.ToTimestamp()) {
			return errors.AssertionFailedf("expiration before lease start")
		}
	case roachpb.LeaseEpoch:
		if nextLease.Expiration != nil {
			return errors.AssertionFailedf("expiration assigned to epoch-based lease")
		}
	default:
		panic("unexpected")
	}
	return nil
}

func validateAcquisitionType(_ BuildInput, nextLease roachpb.Lease) error {
	return validateNonZero(nextLease.AcquisitionType, "acquisition type")
}

func validateSequence(_ BuildInput, nextLease roachpb.Lease) error {
	return validateNonZero(nextLease.Sequence, "sequence")
}

func validateNonZero[T comparable](field T, name string) error {
	var zero T
	if field == zero {
		return errors.AssertionFailedf("%s must be set", name)
	}
	return nil
}
