// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// This file defines wrappers for Reader and Writer, and functions to do the
// wrapping, which depend on the configuration settings above.

// intentDemuxWriter implements 3 methods from the Writer interface:
// PutIntent, ClearIntent, ClearMVCCRangeAndIntents.
type intentDemuxWriter struct {
	w Writer
}

func wrapIntentWriter(w Writer) intentDemuxWriter {
	return intentDemuxWriter{w: w}
}

// ClearIntent has the same behavior as Writer.ClearIntent. buf is used as
// scratch-space to avoid allocations -- its contents will be overwritten and
// not appended to, and a possibly different buf returned.
func (idw intentDemuxWriter) ClearIntent(
	key roachpb.Key, txnDidNotUpdateMeta bool, txnUUID uuid.UUID, buf []byte, opts ClearOptions,
) (_ []byte, _ error) {
	var engineKey EngineKey
	engineKey, buf = LockTableKey{
		Key:      key,
		Strength: lock.Exclusive,
		TxnUUID:  txnUUID,
	}.ToEngineKey(buf)
	if txnDidNotUpdateMeta {
		return buf, idw.w.SingleClearEngineKey(engineKey)
	}
	return buf, idw.w.ClearEngineKey(engineKey, opts)
}

// PutIntent has the same behavior as Writer.PutIntent. buf is used as
// scratch-space to avoid allocations -- its contents will be overwritten and
// not appended to, and a possibly different buf returned.
func (idw intentDemuxWriter) PutIntent(
	ctx context.Context, key roachpb.Key, value []byte, txnUUID uuid.UUID, buf []byte,
) (_ []byte, _ error) {
	var engineKey EngineKey
	engineKey, buf = LockTableKey{
		Key:      key,
		Strength: lock.Exclusive,
		TxnUUID:  txnUUID,
	}.ToEngineKey(buf)
	return buf, idw.w.PutEngineKey(engineKey, value)
}

// ClearMVCCRange has the same behavior as Writer.ClearMVCCRange. buf is used as
// scratch-space to avoid allocations -- its contents will be overwritten and
// not appended to, and a possibly different buf returned.
func (idw intentDemuxWriter) ClearMVCCRange(
	start, end roachpb.Key, pointKeys, rangeKeys bool, buf []byte,
) ([]byte, error) {
	if err := idw.w.ClearRawRange(start, end, pointKeys, rangeKeys); err != nil {
		return buf, err
	}
	// The lock table only contains point keys, so only clear it when point keys
	// are requested, and don't clear range keys in it.
	if !pointKeys {
		return buf, nil
	}
	lstart, buf := keys.LockTableSingleKey(start, buf)
	lend, _ := keys.LockTableSingleKey(end, nil)
	return buf, idw.w.ClearRawRange(lstart, lend, true /* pointKeys */, false /* rangeKeys */)
}
