// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/growstack"
	"google.golang.org/grpc/encoding"
)

type gobEncDecPair struct {
	enc *gob.Encoder
	buf *bytes.Buffer // enc writes to, dec consumes from here
	dec *gob.Decoder
}

func init() {
	gob.Register(&kvpb.RequestUnion_Get{})
	gob.Register(&kvpb.RequestUnion_Put{})
	gob.Register(&kvpb.RequestUnion_ConditionalPut{})
	gob.Register(&kvpb.RequestUnion_Increment{})
	gob.Register(&kvpb.RequestUnion_Delete{})
	gob.Register(&kvpb.RequestUnion_DeleteRange{})
	gob.Register(&kvpb.RequestUnion_ClearRange{})
	gob.Register(&kvpb.RequestUnion_RevertRange{})
	gob.Register(&kvpb.RequestUnion_Scan{})
	gob.Register(&kvpb.RequestUnion_EndTxn{})
	gob.Register(&kvpb.RequestUnion_AdminSplit{})
	gob.Register(&kvpb.RequestUnion_AdminUnsplit{})
	gob.Register(&kvpb.RequestUnion_AdminMerge{})
	gob.Register(&kvpb.RequestUnion_AdminTransferLease{})
	gob.Register(&kvpb.RequestUnion_AdminChangeReplicas{})
	gob.Register(&kvpb.RequestUnion_AdminRelocateRange{})
	gob.Register(&kvpb.RequestUnion_HeartbeatTxn{})
	gob.Register(&kvpb.RequestUnion_Gc{})
	gob.Register(&kvpb.RequestUnion_PushTxn{})
	gob.Register(&kvpb.RequestUnion_RecoverTxn{})
	gob.Register(&kvpb.RequestUnion_ResolveIntent{})
	gob.Register(&kvpb.RequestUnion_ResolveIntentRange{})
	gob.Register(&kvpb.RequestUnion_Merge{})
	gob.Register(&kvpb.RequestUnion_TruncateLog{})
	gob.Register(&kvpb.RequestUnion_RequestLease{})
	gob.Register(&kvpb.RequestUnion_ReverseScan{})
	gob.Register(&kvpb.RequestUnion_ComputeChecksum{})
	gob.Register(&kvpb.RequestUnion_CheckConsistency{})
	gob.Register(&kvpb.RequestUnion_InitPut{})
	gob.Register(&kvpb.RequestUnion_TransferLease{})
	gob.Register(&kvpb.RequestUnion_LeaseInfo{})
	gob.Register(&kvpb.RequestUnion_Export{})
	gob.Register(&kvpb.RequestUnion_QueryTxn{})
	gob.Register(&kvpb.RequestUnion_QueryIntent{})
	gob.Register(&kvpb.RequestUnion_QueryLocks{})
	gob.Register(&kvpb.RequestUnion_AdminScatter{})
	gob.Register(&kvpb.RequestUnion_AddSstable{})
	gob.Register(&kvpb.RequestUnion_RecomputeStats{})
	gob.Register(&kvpb.RequestUnion_Refresh{})
	gob.Register(&kvpb.RequestUnion_RefreshRange{})
	gob.Register(&kvpb.RequestUnion_Subsume{})
	gob.Register(&kvpb.RequestUnion_RangeStats{})
	gob.Register(&kvpb.RequestUnion_AdminVerifyProtectedTimestamp{})
	gob.Register(&kvpb.RequestUnion_Migrate{})
	gob.Register(&kvpb.RequestUnion_QueryResolvedTimestamp{})
	gob.Register(&kvpb.RequestUnion_Barrier{})
	gob.Register(&kvpb.RequestUnion_Probe{})
	gob.Register(&kvpb.RequestUnion_IsSpanEmpty{})
	gob.Register(&kvpb.RequestUnion_LinkExternalSstable{})

	gob.Register(&kvpb.ResponseUnion_Get{})
	gob.Register(&kvpb.ResponseUnion_Put{})
	gob.Register(&kvpb.ResponseUnion_ConditionalPut{})
	gob.Register(&kvpb.ResponseUnion_Increment{})
	gob.Register(&kvpb.ResponseUnion_Delete{})
	gob.Register(&kvpb.ResponseUnion_DeleteRange{})
	gob.Register(&kvpb.ResponseUnion_ClearRange{})
	gob.Register(&kvpb.ResponseUnion_RevertRange{})
	gob.Register(&kvpb.ResponseUnion_Scan{})
	gob.Register(&kvpb.ResponseUnion_EndTxn{})
	gob.Register(&kvpb.ResponseUnion_AdminSplit{})
	gob.Register(&kvpb.ResponseUnion_AdminUnsplit{})
	gob.Register(&kvpb.ResponseUnion_AdminMerge{})
	gob.Register(&kvpb.ResponseUnion_AdminTransferLease{})
	gob.Register(&kvpb.ResponseUnion_AdminChangeReplicas{})
	gob.Register(&kvpb.ResponseUnion_AdminRelocateRange{})
	gob.Register(&kvpb.ResponseUnion_HeartbeatTxn{})
	gob.Register(&kvpb.ResponseUnion_Gc{})
	gob.Register(&kvpb.ResponseUnion_PushTxn{})
	gob.Register(&kvpb.ResponseUnion_RecoverTxn{})
	gob.Register(&kvpb.ResponseUnion_ResolveIntent{})
	gob.Register(&kvpb.ResponseUnion_ResolveIntentRange{})
	gob.Register(&kvpb.ResponseUnion_Merge{})
	gob.Register(&kvpb.ResponseUnion_TruncateLog{})
	gob.Register(&kvpb.ResponseUnion_RequestLease{})
	gob.Register(&kvpb.ResponseUnion_ReverseScan{})
	gob.Register(&kvpb.ResponseUnion_ComputeChecksum{})
	gob.Register(&kvpb.ResponseUnion_CheckConsistency{})
	gob.Register(&kvpb.ResponseUnion_InitPut{})
	gob.Register(&kvpb.ResponseUnion_LeaseInfo{})
	gob.Register(&kvpb.ResponseUnion_Export{})
	gob.Register(&kvpb.ResponseUnion_QueryTxn{})
	gob.Register(&kvpb.ResponseUnion_QueryIntent{})
	gob.Register(&kvpb.ResponseUnion_QueryLocks{})
	gob.Register(&kvpb.ResponseUnion_AdminScatter{})
	gob.Register(&kvpb.ResponseUnion_AddSstable{})
	gob.Register(&kvpb.ResponseUnion_RecomputeStats{})
	gob.Register(&kvpb.ResponseUnion_Refresh{})
	gob.Register(&kvpb.ResponseUnion_RefreshRange{})
	gob.Register(&kvpb.ResponseUnion_Subsume{})
	gob.Register(&kvpb.ResponseUnion_RangeStats{})
	gob.Register(&kvpb.ResponseUnion_AdminVerifyProtectedTimestamp{})
	gob.Register(&kvpb.ResponseUnion_Migrate{})
	gob.Register(&kvpb.ResponseUnion_QueryResolvedTimestamp{})
	gob.Register(&kvpb.ResponseUnion_Barrier{})
	gob.Register(&kvpb.ResponseUnion_Probe{})
	gob.Register(&kvpb.ResponseUnion_IsSpanEmpty{})
	gob.Register(&kvpb.ResponseUnion_LinkExternalSstable{})
}

var allRequestBatchRequest = func() *kvpb.BatchRequest {
	b := new(kvpb.BatchRequest)
	b.Add(&kvpb.GetRequest{})
	b.Add(&kvpb.PutRequest{})
	b.Add(&kvpb.ConditionalPutRequest{})
	b.Add(&kvpb.IncrementRequest{})
	b.Add(&kvpb.DeleteRequest{})
	b.Add(&kvpb.DeleteRangeRequest{})
	b.Add(&kvpb.ClearRangeRequest{})
	b.Add(&kvpb.RevertRangeRequest{})
	b.Add(&kvpb.ScanRequest{})
	b.Add(&kvpb.EndTxnRequest{})
	b.Add(&kvpb.AdminSplitRequest{})
	b.Add(&kvpb.AdminUnsplitRequest{})
	b.Add(&kvpb.AdminMergeRequest{})
	b.Add(&kvpb.AdminTransferLeaseRequest{})
	b.Add(&kvpb.AdminChangeReplicasRequest{})
	b.Add(&kvpb.AdminRelocateRangeRequest{})
	b.Add(&kvpb.HeartbeatTxnRequest{})
	b.Add(&kvpb.GCRequest{})
	b.Add(&kvpb.PushTxnRequest{})
	b.Add(&kvpb.RecoverTxnRequest{})
	b.Add(&kvpb.ResolveIntentRequest{})
	b.Add(&kvpb.ResolveIntentRangeRequest{})
	b.Add(&kvpb.MergeRequest{})
	b.Add(&kvpb.TruncateLogRequest{})
	b.Add(&kvpb.RequestLeaseRequest{})
	b.Add(&kvpb.ReverseScanRequest{})
	b.Add(&kvpb.ComputeChecksumRequest{})
	b.Add(&kvpb.CheckConsistencyRequest{})
	b.Add(&kvpb.InitPutRequest{})
	b.Add(&kvpb.TransferLeaseRequest{})
	b.Add(&kvpb.LeaseInfoRequest{})
	b.Add(&kvpb.ExportRequest{})
	b.Add(&kvpb.QueryTxnRequest{})
	b.Add(&kvpb.QueryIntentRequest{})
	b.Add(&kvpb.QueryLocksRequest{})
	b.Add(&kvpb.AdminScatterRequest{})
	b.Add(&kvpb.AddSSTableRequest{})
	b.Add(&kvpb.RecomputeStatsRequest{})
	b.Add(&kvpb.RefreshRequest{})
	b.Add(&kvpb.RefreshRangeRequest{})
	b.Add(&kvpb.SubsumeRequest{})
	b.Add(&kvpb.RangeStatsRequest{})
	b.Add(&kvpb.AdminVerifyProtectedTimestampRequest{})
	b.Add(&kvpb.MigrateRequest{})
	b.Add(&kvpb.QueryResolvedTimestampRequest{})
	b.Add(&kvpb.BarrierRequest{})
	b.Add(&kvpb.ProbeRequest{})
	b.Add(&kvpb.IsSpanEmptyRequest{})
	b.Add(&kvpb.LinkExternalSSTableRequest{})

	return b
}()

var allRequestBatchResponse = allRequestBatchRequest.CreateReply()

var gobEncDecPairPool = sync.Pool{
	New: func() interface{} {
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		dec := gob.NewDecoder(buf)
		{
			if err := enc.Encode(allRequestBatchRequest); err != nil {
				panic(err)
			}
			if err := dec.Decode(&kvpb.BatchRequest{}); err != nil {
				panic(err)
			}
			buf.Reset()
		}

		{
			if err := enc.Encode(allRequestBatchResponse); err != nil {
				panic(err)
			}
			if err := dec.Decode(&kvpb.BatchResponse{}); err != nil {
				panic(err)
			}
			buf.Reset()
		}
		return &gobEncDecPair{
			enc: enc,
			buf: buf,
			dec: dec,
		}
	},
}

type gobCodec struct {
	fallbackCodec encoding.Codec
}

func (g gobCodec) Name() string {
	// TODO: give this a unique name and use it only for requests that should
	// use it.
	// TODO: cluster version.
	return g.fallbackCodec.Name()
}

func (g gobCodec) Marshal(v interface{}) ([]byte, error) {
	if _, ok := v.(*kvpb.BatchRequest); !ok {
		return g.fallbackCodec.Marshal(v)
	}
	growstack.Grow()
	dep := gobEncDecPairPool.Get().(*gobEncDecPair)
	if dep.buf.Len() > 0 {
		panic("buffer not empty")
	}
	if err := dep.enc.Encode(v); err != nil {
		panic(err)
		// return nil, err
	}
	b := append([]byte(nil), dep.buf.Bytes()...)
	dep.buf.Reset()
	gobEncDecPairPool.Put(dep)
	return b, nil
}

func (g gobCodec) Unmarshal(data []byte, v interface{}) error {
	if _, ok := v.(*kvpb.BatchRequest); !ok {
		return g.fallbackCodec.Unmarshal(data, v)
	}
	dep := gobEncDecPairPool.Get().(*gobEncDecPair)
	// TODO: could save the copy here if we fudged the decoder
	// to read directly from `data`. We'd need to pass a more
	// malleable Writer in at pool.New time. This does not seem
	// hard.
	_, _ = dep.buf.Write(data) // never returns error
	if err := dep.dec.Decode(v); err != nil {
		panic(err)
		// return err
	}
	dep.buf.Reset()
	gobEncDecPairPool.Put(dep)
	return nil
}
