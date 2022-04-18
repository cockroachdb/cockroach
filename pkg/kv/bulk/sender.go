// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type requestOpName string

const (
	// AddSSTableOpName is the operation name for the child span associated with an
	// AddSSTable request.
	AddSSTableOpName requestOpName = "AddSSTable"

	// AdminSplitOpName is the operation name for the child span associated with an
	// AdminSplit request.
	AdminSplitOpName requestOpName = "AdminSplit"

	// AdminScatterOpName is the operation name for the child span associated with an
	// AdminScatter request.
	AdminScatterOpName requestOpName = "AdminScatter"
)

func recordRequestStats(
	sp *tracing.Span, req roachpb.Request, resp roachpb.Response, br *roachpb.BatchResponse,
) {
	if sp == nil {
		return
	}

	switch resp := resp.(type) {
	case *roachpb.AddSSTableResponse:
		storeIDs := make([]roachpb.StoreID, 0)
		if br != nil && len(br.BatchResponse_Header.RangeInfos) > 0 {
			for _, ri := range br.BatchResponse_Header.RangeInfos {
				storeIDs = append(storeIDs, ri.Lease.Replica.StoreID)
			}
		}

		r := req.(*roachpb.AddSSTableRequest)
		sp.RecordStructured(&roachpb.AddSSTableStats{
			DataSize:      int64(sz(len(r.Data))),
			StoreIds:      storeIDs,
			IngestAsWrite: r.IngestAsWrites,
		})

	case *roachpb.AdminScatterResponse:
		if resp.MVCCStats != nil {
			moved := resp.MVCCStats.Total()

			sp.RecordStructured(&roachpb.AdminScatterStats{
				DataMoved: moved,
			})
		}
	default:
	}
}

// SendWrappedWithTracing is a convenience function which wraps the request in a
// batch and sends it via the provided Sender and headers. It returns the batch
// response and unwrapped response or an error. It is not valid to pass a `nil`
// context; since the method is expected to be used with a tracing span that is
// recording.
func SendWrappedWithTracing(
	ctx context.Context,
	sender kv.Sender,
	h roachpb.Header,
	ah roachpb.AdmissionHeader,
	args roachpb.Request,
	opName requestOpName,
) (*roachpb.BatchResponse, roachpb.Response, tracing.Recording, *roachpb.Error) {
	if ctx == nil {
		return nil, nil, nil, roachpb.NewError(errors.New("ctx cannot be nil"))
	}
	ctx, childSpan := tracing.ChildSpan(ctx, string(opName))

	br, rawRes, pErr := kv.SendWrappedWithAdmission(ctx, sender, h, ah, args)
	if pErr != nil {
		childSpan.Finish()
		return nil, nil, nil, pErr
	}

	recordRequestStats(childSpan, args, rawRes, br)
	return br, rawRes, childSpan.FinishAndGetConfiguredRecording(), nil
}

// AdminSplitWithTracing is a utility method that expects a non-nil context and
// returns the trace recording of the child span in which it issues an
// AdminSplitRequest.
func AdminSplitWithTracing(
	ctx context.Context,
	db *kv.DB,
	splitKey interface{},
	expirationTime hlc.Timestamp,
	predicateKeys ...roachpb.Key,
) (tracing.Recording, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	ctx, childSpan := tracing.ChildSpan(ctx, string(AdminSplitOpName))
	err := db.AdminSplit(ctx, splitKey, expirationTime, predicateKeys...)
	if err != nil {
		childSpan.Finish()
		return nil, err
	}

	return childSpan.FinishAndGetConfiguredRecording(), nil
}

// AdminScatterWithTracing is a utility method that expects a non-nil context
// and returns the trace recording of the child span in which it issues an
// AdminScatter request.
func AdminScatterWithTracing(
	ctx context.Context, db *kv.DB, key roachpb.Key, maxSize int64,
) (roachpb.Response, tracing.Recording, error) {
	if ctx == nil {
		return nil, nil, errors.New("ctx cannot be nil")
	}
	ctx, childSpan := tracing.ChildSpan(ctx, string(AdminScatterOpName))
	rawRes, err := db.AdminScatter(ctx, key, maxSize)
	if err != nil {
		childSpan.Finish()
		return nil, nil, err
	}

	recordRequestStats(childSpan, nil /* req */, rawRes, nil /* br */)
	return rawRes, childSpan.FinishAndGetConfiguredRecording(), nil
}
