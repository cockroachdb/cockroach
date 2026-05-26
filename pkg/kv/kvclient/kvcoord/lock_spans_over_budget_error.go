// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
	"github.com/gogo/protobuf/proto"
)

// lockSpansOverBudgetError signals that a txn is being rejected because lock
// spans do not fit in their memory budget.
type lockSpansOverBudgetError struct {
	lockSpansBytes int64
	limitBytes     int64
	baSummary      string
	txnDetails     string
}

func newLockSpansOverBudgetError(
	lockSpansBytes, limitBytes int64, ba *kvpb.BatchRequest,
) lockSpansOverBudgetError {
	return lockSpansOverBudgetError{
		lockSpansBytes: lockSpansBytes,
		limitBytes:     limitBytes,
		baSummary:      ba.Summary(),
		txnDetails:     ba.Txn.String(),
	}
}

func (l lockSpansOverBudgetError) Error() string {
	return fmt.Sprintf("the transaction is locking too many rows and exceeded its lock-tracking memory budget; "+
		"lock spans: %d bytes > budget: %d bytes. Request pushing transaction over the edge: %s. "+
		"Transaction details: %s.", l.lockSpansBytes, l.limitBytes, l.baSummary, l.txnDetails)
}

func encodeLockSpansOverBudgetError(
	_ context.Context, err error,
) (msgPrefix string, safe []string, details proto.Message) {
	t := err.(lockSpansOverBudgetError)
	details = &errorspb.StringsPayload{
		Details: []string{
			strconv.FormatInt(t.lockSpansBytes, 10), strconv.FormatInt(t.limitBytes, 10),
			t.baSummary, t.txnDetails,
		},
	}
	msgPrefix = "the transaction is locking too many rows"
	return msgPrefix, nil, details
}

func decodeLockSpansOverBudgetError(
	_ context.Context, msgPrefix string, safeDetails []string, payload proto.Message,
) error {
	m, ok := payload.(*errorspb.StringsPayload)
	if !ok || len(m.Details) < 4 {
		// If this ever happens, this means some version of the library
		// (presumably future) changed the payload type, and we're
		// receiving this here. In this case, give up and let
		// DecodeError use the opaque type.
		return nil
	}
	lockBytes, decodeErr := strconv.ParseInt(m.Details[0], 10, 64)
	if decodeErr != nil {
		return nil //nolint:returnerrcheck
	}
	limitBytes, decodeErr := strconv.ParseInt(m.Details[1], 10, 64)
	if decodeErr != nil {
		return nil //nolint:returnerrcheck
	}
	return lockSpansOverBudgetError{
		lockSpansBytes: lockBytes,
		limitBytes:     limitBytes,
		baSummary:      m.Details[2],
		txnDetails:     m.Details[3],
	}
}

func init() {
	pKey := errors.GetTypeKey(lockSpansOverBudgetError{})
	errors.RegisterLeafEncoder(pKey, encodeLockSpansOverBudgetError)
	errors.RegisterLeafDecoder(pKey, decodeLockSpansOverBudgetError)
}
