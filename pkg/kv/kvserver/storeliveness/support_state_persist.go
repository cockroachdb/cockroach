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
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func readRequesterMeta(ctx context.Context, r storage.Reader) (slpb.RequesterMeta, error) {
	var rm slpb.RequesterMeta
	key := keys.StoreLivenessRequesterMetaKey()
	err := readProto(ctx, r, key, &rm)
	return rm, err
}

func readSupporterMeta(ctx context.Context, r storage.Reader) (slpb.SupporterMeta, error) {
	var sm slpb.SupporterMeta
	key := keys.StoreLivenessSupporterMetaKey()
	err := readProto(ctx, r, key, &sm)
	return sm, err
}

func readProto(
	ctx context.Context, r storage.Reader, key roachpb.Key, msg protoutil.Message,
) error {
	_, err := storage.MVCCGetProto(
		ctx,
		r,
		key,
		hlc.Timestamp{},
		msg,
		storage.MVCCGetOptions{},
	)
	return err
}

func readSupportForState(ctx context.Context, r storage.Reader) ([]slpb.SupportState, error) {
	var sss []slpb.SupportState
	minKey := keys.StoreLivenessSupportForKey(0, 0)
	maxKey := keys.StoreLivenessSupportForKey(math.MaxInt32, math.MaxInt32)
	_, err := storage.MVCCIterate(ctx, r, minKey, maxKey, hlc.Timestamp{}, storage.MVCCScanOptions{},
		func(kv roachpb.KeyValue) error {
			sss = append(sss, slpb.SupportState{})
			ss := &sss[len(sss)-1]
			return kv.Value.GetProto(ss)
		})
	return sss, err
}

func writeRequesterMeta(ctx context.Context, rw storage.ReadWriter, rm slpb.RequesterMeta) error {
	key := keys.StoreLivenessRequesterMetaKey()
	return writeProto(ctx, rw, key, &rm)
}

func writeSupporterMeta(ctx context.Context, rw storage.ReadWriter, sm slpb.SupporterMeta) error {
	key := keys.StoreLivenessSupporterMetaKey()
	return writeProto(ctx, rw, key, &sm)
}

func writeSupportForState(ctx context.Context, rw storage.ReadWriter, ss slpb.SupportState) error {
	key := keys.StoreLivenessSupportForKey(ss.Target.NodeID, ss.Target.StoreID)
	return writeProto(ctx, rw, key, &ss)
}

func writeProto(
	ctx context.Context, rw storage.ReadWriter, key roachpb.Key, msg protoutil.Message,
) error {
	return storage.MVCCPutProto(
		ctx,
		rw,
		key,
		hlc.Timestamp{},
		msg,
		storage.MVCCWriteOptions{},
	)
}
