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

const (
	// AddSSTableOpName is the operation name for the child span associated with an
	// AddSSTable request.
	AddSSTableOpName = "AddSSTable"

	// AdminSplitOpName is the operation name for the child span associated with an
	// AdminSplit request.
	AdminSplitOpName = "AdminSplit"

	// AdminScatterOpName is the operation name for the child span associated with an
	// AdminScatter request.
	AdminScatterOpName = "AdminScatter"
)

// recordRequestStats adds a StructuredRecord to the tracing span sp using
// information from the passed in request and response.
//
// req can be nil in cases where it does not contain information that needs to
// be recorded. We expect sp, and br to be non-nil.
func recordRequestStats(sp *tracing.Span, req roachpb.Request, br *roachpb.BatchResponse) {
	if sp == nil {
		return
	}

	for _, ru := range br.Responses {
		switch resp := ru.GetInner().(type) {
		case *roachpb.AddSSTableResponse:
			storeIDs := make([]roachpb.StoreID, 0)
			if br != nil {
				for _, ri := range br.BatchResponse_Header.RangeInfos {
					storeIDs = append(storeIDs, ri.Lease.Replica.StoreID)
				}
			}

			r := req.(*roachpb.AddSSTableRequest)
			sp.RecordStructured(&roachpb.AddSSTableStats{
				DataSizeBytes: int64(sz(len(r.Data))),
				Stores:        storeIDs,
				IngestAsWrite: r.IngestAsWrites,
			})

		case *roachpb.AdminScatterResponse:
			if resp.MVCCStats != nil {
				sp.RecordStructured(&roachpb.AdminScatterStats{
					DataInScatteredRangesBytes: resp.MVCCStats.Total(),
				})
			}

		default:
		}
	}
}

// SendWrappedWithTracing is a convenience function which wraps the request in a
// batch and sends it via the provided Sender and headers. It returns the batch
// response and unwrapped response or an error. It is not valid to pass a `nil`
// context; since the method is expected to be used with a tracing span.
//
// The recording mode used when fetching the span recording is specified using
// recordingType.
func SendWrappedWithTracing(
	ctx context.Context,
	sender kv.Sender,
	h roachpb.Header,
	ah roachpb.AdmissionHeader,
	args roachpb.Request,
	recordingType tracing.RecordingType,
) (*roachpb.BatchResponse, roachpb.Response, tracing.Recording, *roachpb.Error) {
	if ctx == nil {
		return nil, nil, nil, roachpb.NewError(errors.New("ctx cannot be nil"))
	}

	var opName string
	switch args.(type) {
	case *roachpb.AddSSTableRequest:
		opName = AddSSTableOpName
	}
	ctx, childSpan := tracing.ChildSpan(ctx, opName)

	ba := roachpb.BatchRequest{}
	ba.Header = h
	ba.AdmissionHeader = ah
	ba.Add(args)

	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		childSpan.Finish()
		return nil, nil, nil, pErr
	}
	unwrappedReply := br.Responses[0].GetInner()
	header := unwrappedReply.Header()
	header.Txn = br.Txn
	unwrappedReply.SetHeader(header)

	recordRequestStats(childSpan, args, br)
	return br, unwrappedReply, childSpan.FinishAndGetRecording(recordingType), nil
}

// adminSplitWithTracing is a utility method that expects a non-nil context and
// returns the trace recording of the child span in which it issues an
// AdminSplitRequest.
//
// The recording mode used when fetching the span recording is specified using
// recordingType.
func adminSplitWithTracing(
	ctx context.Context,
	db *kv.DB,
	splitKey interface{},
	expirationTime hlc.Timestamp,
	recordingType tracing.RecordingType,
	predicateKeys ...roachpb.Key,
) (tracing.Recording, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	ctx, childSpan := tracing.ChildSpan(ctx, AdminSplitOpName)
	err := db.AdminSplit(ctx, splitKey, expirationTime, predicateKeys...)
	if err != nil {
		childSpan.Finish()
		return nil, err
	}

	return childSpan.FinishAndGetRecording(recordingType), nil
}

// adminScatterWithTracing is a utility method that expects a non-nil context
// and returns the trace recording of the child span in which it issues an
// AdminScatter request.
//
// The recording mode used when fetching the span recording is specified using
// recordingType.
func adminScatterWithTracing(
	ctx context.Context,
	db *kv.DB,
	key roachpb.Key,
	maxSize int64,
	recordingType tracing.RecordingType,
) (roachpb.Response, tracing.Recording, error) {
	if ctx == nil {
		return nil, nil, errors.New("ctx cannot be nil")
	}
	ctx, childSpan := tracing.ChildSpan(ctx, AdminScatterOpName)
	rawRes, err := db.AdminScatter(ctx, key, maxSize)
	if err != nil {
		childSpan.Finish()
		return nil, nil, err
	}

	var union roachpb.ResponseUnion
	union.MustSetInner(rawRes)
	recordRequestStats(childSpan, nil, /* req */
		&roachpb.BatchResponse{Responses: []roachpb.ResponseUnion{union}})
	return rawRes, childSpan.FinishAndGetRecording(recordingType), nil
}
