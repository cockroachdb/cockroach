package ptverifier

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

func Verify(ctx context.Context, ds *kv.DistSender, r *ptpb.Record, createdAt hlc.Timestamp) error {
	// For all of the spans in the record we need to go reach out to all of the ranges which cover that span.
	// We're going to do this in an iterative way by finding the ranges, sending them
	//
	// AdminVerifyProtectedTimestampRequests then waiting for responses which will include range info.

	ri := kv.NewRangeIterator(ds)
	var ba roachpb.BatchRequest
	var req roachpb.AdminVerifyProtectedTimestampRequest
	mergedSpans, _ := roachpb.MergeSpans(r.Spans)
	remaining := roachpb.Spans(mergedSpans)

	var done roachpb.Spans
	for len(remaining) > 0 {
		for _, s := range remaining {
			// TODO(ajwerner): deal with adressing.
			rs := roachpb.RSpan{
				Key:    roachpb.RKey(s.Key),
				EndKey: roachpb.RKey(s.EndKey),
			}
			ri.Seek(ctx, rs.Key, kv.Ascending)
			for ri.Valid() {
				desc := ri.Desc()
				req.Reset()
				ba.Reset()
				req.Key = roachpb.Key(desc.StartKey)
				req.EndKey = roachpb.Key(desc.EndKey)
				ba.Header.ReturnRangeInfo = true
				ba.Add(&req)
				br, pErr := ds.Send(ctx, ba)
				if pErr != nil {
					return errors.Wrapf(pErr.GoError(), "failed to verify protection on %v", desc)
				}
				resp := br.Responses[0].GetAdminVerifyProtectedTimestamp()
				if !resp.Verified {
					return errors.Errorf("failed to verify protection on %v", desc)
				}
				for _, ri := range resp.RangeInfos {
					done = append(done, ri.Desc.RSpan().AsRawSpanWithNoLocals())
				}
				if ri.NeedAnother(rs) {
					ri.Next(ctx)
				} else {
					break
				}
			}
		}
		remaining, done = roachpb.SubtractSpans(remaining, done), done[:0]
	}
	return nil
}
