// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// readRequesterMeta reads the RequesterMeta from disk.
func readRequesterMeta(ctx context.Context, r storage.Reader) (slpb.RequesterMeta, error) {
	var rm slpb.RequesterMeta
	key := keys.StoreLivenessRequesterMetaKey()
	err := readProto(ctx, r, key, &rm)
	return rm, err
}

// readSupporterMeta reads the SupporterMeta from disk.
func readSupporterMeta(ctx context.Context, r storage.Reader) (slpb.SupporterMeta, error) {
	var sm slpb.SupporterMeta
	key := keys.StoreLivenessSupporterMetaKey()
	err := readProto(ctx, r, key, &sm)
	return sm, err
}

func readProto(
	ctx context.Context, r storage.Reader, key roachpb.Key, msg protoutil.Message,
) error {
	_, err := storage.MVCCGetProto(ctx, r, key, hlc.Timestamp{}, msg, storage.MVCCGetOptions{})
	return err
}

// readSupportForState iterates over all StoreLivenessSupportForKeys and reads
// SupportStates stored to disk. Before returning each of them, it populates the
// Target field with the store identifier from the key.
func readSupportForState(ctx context.Context, r storage.Reader) ([]slpb.SupportState, error) {
	var sss []slpb.SupportState
	minKey := keys.StoreLivenessSupportForKey(0, 0)
	maxKey := keys.StoreLivenessSupportForKey(math.MaxInt32, math.MaxInt32)
	_, err := storage.MVCCIterate(
		ctx, r, minKey, maxKey, hlc.Timestamp{},
		storage.MVCCScanOptions{}, func(kv roachpb.KeyValue) error {
			sss = append(sss, slpb.SupportState{})
			ss := &sss[len(sss)-1]
			if err := kv.Value.GetProto(ss); err != nil {
				return err
			}
			nodeID, storeID, err := keys.DecodeStoreLivenessSupportForKey(kv.Key)
			if err != nil {
				return err
			}
			ss.Target = slpb.StoreIdent{NodeID: nodeID, StoreID: storeID}
			return nil
		},
	)
	return sss, err
}

// writeRequesterMeta writes the RequesterMeta to disk.
func writeRequesterMeta(ctx context.Context, rw storage.ReadWriter, rm slpb.RequesterMeta) error {
	key := keys.StoreLivenessRequesterMetaKey()
	return writeProto(ctx, rw, key, &rm)
}

// writeSupporterMeta writes the SupporterMeta to disk.
func writeSupporterMeta(ctx context.Context, rw storage.ReadWriter, sm slpb.SupporterMeta) error {
	key := keys.StoreLivenessSupporterMetaKey()
	return writeProto(ctx, rw, key, &sm)
}

// writeSupportForState writes a single SupportState corresponding to a
// supportFor entry. Before doing so, as an optimization, it clears the Target
// field; the store identifier is available in StoreLivenessSupportForKey, and
// it is populated into Target again upon read.
func writeSupportForState(ctx context.Context, rw storage.ReadWriter, ss slpb.SupportState) error {
	key := keys.StoreLivenessSupportForKey(ss.Target.NodeID, ss.Target.StoreID)
	ss.Target.Reset()
	return writeProto(ctx, rw, key, &ss)
}

func writeProto(
	ctx context.Context, rw storage.ReadWriter, key roachpb.Key, msg protoutil.Message,
) error {
	return storage.MVCCPutProto(ctx, rw, key, hlc.Timestamp{}, msg, storage.MVCCWriteOptions{})
}
