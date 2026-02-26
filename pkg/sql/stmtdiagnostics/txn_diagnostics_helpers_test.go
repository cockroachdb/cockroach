// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stmtdiagnostics

import (
	"context"
	"time"
)

func (r *TxnRegistry) GetRequest(requestID RequestID) (TxnRequest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	req, ok := r.mu.requests[requestID]
	return req, ok
}

func (r *TxnRegistry) GetRequestForFingerprint(
	txnFingerprintID uint64,
) (RequestID, TxnRequest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, req := range r.mu.requests {
		if req.txnFingerprintId == txnFingerprintID {
			return id, req, true
		}
	}
	return 0, TxnRequest{}, false
}

func (r *TxnRegistry) InsertTxnRequestInternal(
	ctx context.Context,
	txnFingerprintId uint64,
	stmtFingerprintIds []uint64,
	username string,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAfter time.Duration,
	redacted bool,
) (RequestID, error) {
	id, err := r.insertTxnRequestInternal(
		ctx, txnFingerprintId, stmtFingerprintIds, username, samplingProbability, minExecutionLatency, expiresAfter, redacted)
	return id, err
}
