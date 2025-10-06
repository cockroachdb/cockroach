// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package liveness

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// Storage is the subset of liveness that deals with reading and writing the
// liveness records to kv. All calls to modify liveness are centralized here.
type Storage interface {
	Get(ctx context.Context, nodeID roachpb.NodeID) (Record, error)
	Update(
		ctx context.Context, update LivenessUpdate, handleCondFailed func(actual Record) error,
	) (Record, error)
	Create(ctx context.Context, nodeID roachpb.NodeID) error
	Scan(ctx context.Context) ([]Record, error)
}

// storageImpl implements Storage by storing entries in the replicated KV liveness range.
type storageImpl struct {
	db *kv.DB
}

var _ Storage = (*storageImpl)(nil)

// NewKVStorage returns a Storage backed by the node liveness range.
func NewKVStorage(db *kv.DB) Storage {
	return &storageImpl{db}
}

// LivenessUpdate contains the information for CPutting a new version of a
// liveness record. It has both the new and the old version of the proto.
type LivenessUpdate struct {
	newLiveness livenesspb.Liveness
	oldLiveness livenesspb.Liveness
	// oldRaw is the raw value from which `old` was decoded. Used for CPuts as the
	// existing value. Note that we don't simply marshal `old` as that would break
	// if unmarshalling/marshaling doesn't round-trip. Nil means that a liveness
	// record for the respected node is not expected to exist in the database.
	oldRaw []byte
}

// Get returns a slice containing the liveness record of all nodes that have
// ever been a part of the cluster. The records are read from the KV layer in a
// KV transaction.
//
// NB: Normally the liveness record should not be directly read from KV, and
// instead from an in-memory cache. Reading from KV is more expensive and
// typically unnecessary. For updating liveness, it is still not necessary to
// call get, since the handleCondFailed after a CPut will notify you of the
// previous data.
func (ls *storageImpl) Get(ctx context.Context, nodeID roachpb.NodeID) (Record, error) {
	var oldLiveness livenesspb.Liveness
	record, err := ls.db.Get(ctx, keys.NodeLivenessKey(nodeID))
	if err != nil {
		return Record{}, errors.Wrap(err, "unable to get liveness")
	}
	if record.Value == nil {
		return Record{}, ErrMissingRecord
	}
	if err := record.Value.GetProto(&oldLiveness); err != nil {
		return Record{}, errors.Wrap(err, "invalid liveness record")
	}

	return Record{
		Liveness: oldLiveness,
		raw:      record.Value.TagAndDataBytes(),
	}, nil
}

// Update will attempt to update the liveness record using a CPut with the
// oldRaw from the LivenessUpdate. If the oldRaw does not match, the
// handleCondFailed func is called with the current data stored for this node.
// This method does not retry, but normally the caller will retry using the
// returned value on a condition failure.
func (ls *storageImpl) Update(
	ctx context.Context, update LivenessUpdate, handleCondFailed func(actual Record) error,
) (Record, error) {
	var v *roachpb.Value
	if err := ls.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// NB: we have to allocate a new Value every time because once we've
		// update a value into the KV API we have to assume something hangs on
		// to it still.
		v = new(roachpb.Value)

		b := txn.NewBatch()
		key := keys.NodeLivenessKey(update.newLiveness.NodeID)
		if err := v.SetProto(&update.newLiveness); err != nil {
			log.Fatalf(ctx, "failed to marshall proto: %s", err)
		}
		b.CPut(key, v, update.oldRaw)
		// Use a trigger on EndTxn to indicate that node liveness should be
		// re-gossiped. Further, require that this transaction complete as a one
		// phase commit to eliminate the possibility of leaving write intents.
		b.AddRawRequest(&kvpb.EndTxnRequest{
			Commit:     true,
			Require1PC: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{
					NodeLivenessSpan: &roachpb.Span{
						Key:    key,
						EndKey: key.Next(),
					},
				},
			},
		})
		return txn.Run(ctx, b)
	}); err != nil {
		if tErr := (*kvpb.ConditionFailedError)(nil); errors.As(err, &tErr) {
			if tErr.ActualValue == nil {
				return Record{}, handleCondFailed(Record{})
			}
			var actualLiveness livenesspb.Liveness
			if err := tErr.ActualValue.GetProto(&actualLiveness); err != nil {
				return Record{}, errors.Wrapf(err, "couldn't update node liveness from CPut actual value")
			}
			return Record{}, handleCondFailed(Record{Liveness: actualLiveness, raw: tErr.ActualValue.TagAndDataBytes()})
		} else if isErrRetryLiveness(ctx, err) {
			return Record{}, &errRetryLiveness{err}
		}
		return Record{}, err
	}

	return Record{Liveness: update.newLiveness, raw: v.TagAndDataBytes()}, nil
}

// Create creates a liveness record for the node specified by the
// given node ID. This is typically used when adding a new node to a running
// cluster, or when bootstrapping a cluster through a given node.
//
// NB: An existing liveness record is not overwritten by this method, we return
// an error instead.
func (ls *storageImpl) Create(ctx context.Context, nodeID roachpb.NodeID) error {
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		// We start off at epoch=0, entrusting the initial heartbeat to increment it
		// to epoch=1 to signal the very first time the node is up and running.
		liveness := livenesspb.Liveness{NodeID: nodeID, Epoch: 0}

		// We skip adding an expiration, we only really care about the liveness
		// record existing within KV.

		v := new(roachpb.Value)
		err := ls.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			b := txn.NewBatch()
			key := keys.NodeLivenessKey(nodeID)
			if err := v.SetProto(&liveness); err != nil {
				log.Fatalf(ctx, "failed to marshall proto: %s", err)
			}
			// Given we're looking to create a new liveness record here, we don't
			// expect to find anything.
			b.CPut(key, v, nil)

			// We don't bother adding a gossip trigger, that'll happen with the
			// first heartbeat. We still keep it as a 1PC commit to avoid leaving
			// write intents.
			b.AddRawRequest(&kvpb.EndTxnRequest{
				Commit:     true,
				Require1PC: true,
			})
			return txn.Run(ctx, b)
		})

		if err == nil {
			// We'll learn about this liveness record through gossip eventually, so we
			// don't bother updating our in-memory view of node liveness.
			log.Infof(ctx, "created liveness record for n%d", nodeID)
			return nil
		}
		if !isErrRetryLiveness(ctx, err) {
			return err
		}
		log.VEventf(ctx, 2, "failed to create liveness record for node %d, because of %s. retrying...", nodeID, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.AssertionFailedf("unexpected problem while creating liveness record for node %d", nodeID)
}

// Scan will iterate over the KV liveness names and generate liveness records from them.
func (ls *storageImpl) Scan(ctx context.Context) ([]Record, error) {
	kvs, err := ls.db.Scan(ctx, keys.NodeLivenessPrefix, keys.NodeLivenessKeyMax, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get liveness")
	}
	var results []Record
	for _, kv := range kvs {
		if kv.Value == nil {
			return nil, errors.AssertionFailedf("missing liveness record")
		}
		var liveness livenesspb.Liveness
		if err := kv.Value.GetProto(&liveness); err != nil {
			return nil, errors.Wrap(err, "invalid liveness record")
		}

		results = append(results, Record{
			Liveness: liveness,
			raw:      kv.Value.TagAndDataBytes(),
		})
	}

	return results, nil
}
