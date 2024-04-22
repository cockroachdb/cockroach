// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
)

type txnWriteBuffer struct {
	wrapped lockedSender
	buf     btree
}

// SendLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if true {
		return twb.wrapped.SendLocked(ctx, ba)
	}
	//TODO implement me
	panic("implement me")
}

// setWrapped implements the txnInterceptor interface.
func (twb *txnWriteBuffer) setWrapped(wrapped lockedSender) {
	twb.wrapped = wrapped
}

// populateLeafInputState implements the txnInterceptor interface.
func (twb *txnWriteBuffer) populateLeafInputState(*roachpb.LeafTxnInputState) {
	// TODO(nvanbenschoten): send buffered writes to LeafTxns.
}

// populateLeafFinalState implements the txnInterceptor interface.
func (twb *txnWriteBuffer) populateLeafFinalState(*roachpb.LeafTxnFinalState) {
	// TODO(nvanbenschoten): ingest buffered writes in LeafTxns.
}

// importLeafFinalState implements the txnInterceptor interface.
func (twb *txnWriteBuffer) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) epochBumpedLocked() {
	twb.buf.Reset()
}

// createSavepointLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) createSavepointLocked(ctx context.Context, s *savepoint) {}

// rollbackToSavepointLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) rollbackToSavepointLocked(ctx context.Context, s savepoint) {
	// TODO(nvanbenschoten): clear out writes after the savepoint.
}

// closeLocked implements the txnInterceptor interface.
func (twb *txnWriteBuffer) closeLocked() {
	twb.buf.Reset()
}

// bufferedWrite is a key-value pair with an associated sequence number.
type bufferedWrite struct {
	id     uint64
	kv     roachpb.KeyValue
	seq    enginepb.TxnSeq
	endKey roachpb.Key // used in btree iteration
}

//go:generate ../../../util/interval/generic/gen.sh *bufferedWrite kvcoord

// Methods required by util/interval/generic type contract.
func (bw *bufferedWrite) ID() uint64          { return bw.id }
func (bw *bufferedWrite) Key() []byte         { return bw.kv.Key }
func (bw *bufferedWrite) EndKey() []byte      { return bw.endKey }
func (bw *bufferedWrite) String() string      { return "todo" }
func (bw *bufferedWrite) New() *bufferedWrite { return new(bufferedWrite) }
func (bw *bufferedWrite) SetID(v uint64)      { bw.id = v }
func (bw *bufferedWrite) SetKey(v []byte)     { bw.kv.Key = v }
func (bw *bufferedWrite) SetEndKey(v []byte)  { bw.endKey = v }
