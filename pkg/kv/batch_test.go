// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBatch_ApproximateMutationBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	require.Equal(t, 0, b.ApproximateMutationBytes())

	b.Put("key1", "value1")
	require.Greater(t, b.ApproximateMutationBytes(), len("key1")+len("value1"))

	bytesAfterFirstPut := b.ApproximateMutationBytes()
	b.Put("key2", "value2")
	require.Greater(t, b.ApproximateMutationBytes(), bytesAfterFirstPut)

	bytesAfterSecondPut := b.ApproximateMutationBytes()
	b.Del("key3")
	require.Greater(t, b.ApproximateMutationBytes(), bytesAfterSecondPut)
}

func TestBatch_validate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	require.NoError(t, b.validate())

	b.Results = []Result{{Err: errors.New("test error")}}
	err := b.validate()
	require.Error(t, err)
	require.Equal(t, "test error", err.Error())
	require.NotNil(t, b.pErr)
}

func TestBatch_initResult(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("basic", func(t *testing.T) {
		b := &Batch{}
		b.initResult(1, 2, notRaw, nil)
		require.Len(t, b.Results, 1)
		require.Equal(t, 1, b.Results[0].calls)
		require.Len(t, b.Results[0].Rows, 2)
		require.NoError(t, b.Results[0].Err)
	})

	t.Run("with error", func(t *testing.T) {
		b := &Batch{}
		testErr := errors.New("test error")
		b.initResult(1, 1, notRaw, testErr)
		require.Len(t, b.Results, 1)
		require.Equal(t, testErr, b.Results[0].Err)
	})

	t.Run("raw batch with non-raw operation", func(t *testing.T) {
		b := &Batch{raw: true}
		b.initResult(1, 1, notRaw, nil)
		require.Len(t, b.Results, 1)
		require.Error(t, b.Results[0].Err)
		require.Contains(t, b.Results[0].Err.Error(), "must not use non-raw operations on a raw batch")
	})

}

func TestBatch_AddRawRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	req := kvpb.NewGet(roachpb.Key("key"))
	b.AddRawRequest(req)

	require.True(t, b.raw)
	require.Len(t, b.reqs, 1)
	require.Len(t, b.Results, 1)
	require.Equal(t, 1, b.Results[0].calls)

	req2 := kvpb.NewPut(roachpb.Key("key2"), roachpb.Value{})
	b.AddRawRequest(req2)
	require.Len(t, b.reqs, 2)
	require.Len(t, b.Results, 2)
}

func TestBatch_Get(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	b.Get("key")

	require.Len(t, b.reqs, 1)
	require.Len(t, b.Results, 1)
	require.IsType(t, &kvpb.GetRequest{}, b.reqs[0].GetInner())

	getReq := b.reqs[0].GetInner().(*kvpb.GetRequest)
	require.Equal(t, roachpb.Key("key"), getReq.Key)
	require.Equal(t, lock.None, getReq.KeyLockingStrength)
}

func TestBatch_Put(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	b.Put("key", "value")

	require.Len(t, b.reqs, 1)
	require.Len(t, b.Results, 1)
	require.IsType(t, &kvpb.PutRequest{}, b.reqs[0].GetInner())

	putReq := b.reqs[0].GetInner().(*kvpb.PutRequest)
	require.Equal(t, roachpb.Key("key"), putReq.Key)
	val, err := putReq.Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, "value", string(val))
	require.Greater(t, b.approxMutationReqBytes, 0)
}

func TestBatch_CPut(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	expValue := []byte("expected")
	b.CPut("key", "value", expValue)

	require.Len(t, b.reqs, 1)
	require.IsType(t, &kvpb.ConditionalPutRequest{}, b.reqs[0].GetInner())

	cputReq := b.reqs[0].GetInner().(*kvpb.ConditionalPutRequest)
	require.Equal(t, roachpb.Key("key"), cputReq.Key)
	val, err := cputReq.Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, "value", string(val))
	require.Equal(t, expValue, cputReq.ExpBytes)
	require.False(t, cputReq.AllowIfDoesNotExist)
}

func TestBatch_Inc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	b.Inc("key", 42)

	require.Len(t, b.reqs, 1)
	require.IsType(t, &kvpb.IncrementRequest{}, b.reqs[0].GetInner())

	incReq := b.reqs[0].GetInner().(*kvpb.IncrementRequest)
	require.Equal(t, roachpb.Key("key"), incReq.Key)
	require.Equal(t, int64(42), incReq.Increment)
}

func TestBatch_Scan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	b.Scan("start", "end")

	require.Len(t, b.reqs, 1)
	require.IsType(t, &kvpb.ScanRequest{}, b.reqs[0].GetInner())

	scanReq := b.reqs[0].GetInner().(*kvpb.ScanRequest)
	require.Equal(t, roachpb.Key("start"), scanReq.Key)
	require.Equal(t, roachpb.Key("end"), scanReq.EndKey)
	require.Equal(t, lock.None, scanReq.KeyLockingStrength)
}

func TestBatch_ReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	b.ReverseScan("start", "end")

	require.Len(t, b.reqs, 1)
	require.IsType(t, &kvpb.ReverseScanRequest{}, b.reqs[0].GetInner())

	scanReq := b.reqs[0].GetInner().(*kvpb.ReverseScanRequest)
	require.Equal(t, roachpb.Key("start"), scanReq.Key)
	require.Equal(t, roachpb.Key("end"), scanReq.EndKey)
}

func TestBatch_Del(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	b.Del("key1", "key2")

	require.Len(t, b.reqs, 2)
	require.Len(t, b.Results, 1)

	delReq1 := b.reqs[0].GetInner().(*kvpb.DeleteRequest)
	delReq2 := b.reqs[1].GetInner().(*kvpb.DeleteRequest)
	require.Equal(t, roachpb.Key("key1"), delReq1.Key)
	require.Equal(t, roachpb.Key("key2"), delReq2.Key)
	require.False(t, delReq1.MustAcquireExclusiveLock)
}

func TestBatch_DelRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	b.DelRange("start", "end", true)

	require.Len(t, b.reqs, 1)
	require.IsType(t, &kvpb.DeleteRangeRequest{}, b.reqs[0].GetInner())

	delRangeReq := b.reqs[0].GetInner().(*kvpb.DeleteRangeRequest)
	require.Equal(t, roachpb.Key("start"), delRangeReq.Key)
	require.Equal(t, roachpb.Key("end"), delRangeReq.EndKey)
	require.True(t, delRangeReq.ReturnKeys)
	require.False(t, delRangeReq.UseRangeTombstone)
}

func TestBatch_GetResult(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	b.Get("key1")

	result, expBytes, _, err := b.GetResult(0)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Nil(t, expBytes)
	// The KeyValue will be empty since we haven't run the batch,
	// but the Result should have the right number of calls and rows
	require.Equal(t, 1, result.calls)
	require.Len(t, result.Rows, 1)

	_, _, _, err = b.GetResult(1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "index 1 outside of results")
}

func TestBatch_fillResults(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("raw batch is no-op", func(t *testing.T) {
		b := &Batch{raw: true}
		b.fillResults(ctx)
	})

	t.Run("get request", func(t *testing.T) {
		b := &Batch{}
		b.Get("key1")

		br := &kvpb.BatchResponse{}
		br.Responses = make([]kvpb.ResponseUnion, 1)
		getResp := &kvpb.GetResponse{}
		getResp.Value = &roachpb.Value{}
		getResp.Value.SetString("value1")
		br.Responses[0].MustSetInner(getResp)

		b.response = br
		b.fillResults(ctx)

		require.Len(t, b.Results, 1)
		require.Len(t, b.Results[0].Rows, 1)
		require.Equal(t, roachpb.Key("key1"), b.Results[0].Rows[0].Key)
		require.Equal(t, []byte("value1"), b.Results[0].Rows[0].ValueBytes())
	})

	t.Run("multiple calls per result", func(t *testing.T) {
		b := &Batch{}
		b.Del("key1", "key2")

		br := &kvpb.BatchResponse{}
		br.Responses = make([]kvpb.ResponseUnion, 2)
		delResp1 := &kvpb.DeleteResponse{FoundKey: true}
		delResp2 := &kvpb.DeleteResponse{FoundKey: true}
		br.Responses[0].MustSetInner(delResp1)
		br.Responses[1].MustSetInner(delResp2)

		b.response = br
		b.fillResults(ctx)

		require.Len(t, b.Results, 1)
		require.Len(t, b.Results[0].Keys, 2)
		require.Equal(t, roachpb.Key("key1"), b.Results[0].Keys[0])
		require.Equal(t, roachpb.Key("key2"), b.Results[0].Keys[1])
	})

	t.Run("panic on insufficient responses", func(t *testing.T) {
		b := &Batch{}
		b.Get("key1")

		br := &kvpb.BatchResponse{}
		br.Responses = make([]kvpb.ResponseUnion, 0)

		b.response = br
		require.Panics(t, func() {
			b.fillResults(ctx)
		})
	})

	t.Run("scan request", func(t *testing.T) {
		b := &Batch{}
		b.Scan("start", "end")

		br := &kvpb.BatchResponse{}
		br.Responses = make([]kvpb.ResponseUnion, 1)
		scanResp := &kvpb.ScanResponse{}
		kv1 := roachpb.KeyValue{Key: roachpb.Key("key1")}
		kv1.Value.SetString("value1")
		kv2 := roachpb.KeyValue{Key: roachpb.Key("key2")}
		kv2.Value.SetString("value2")
		scanResp.Rows = []roachpb.KeyValue{kv1, kv2}
		br.Responses[0].MustSetInner(scanResp)

		b.response = br
		b.fillResults(ctx)

		require.Len(t, b.Results, 1)
		require.Len(t, b.Results[0].Rows, 2)
		require.Equal(t, roachpb.Key("key1"), b.Results[0].Rows[0].Key)
		require.Equal(t, []byte("value1"), b.Results[0].Rows[0].ValueBytes())
		require.Equal(t, roachpb.Key("key2"), b.Results[0].Rows[1].Key)
		require.Equal(t, []byte("value2"), b.Results[0].Rows[1].ValueBytes())
	})

	t.Run("with batch error", func(t *testing.T) {
		b := &Batch{}
		b.Get("key1")

		pErr := kvpb.NewErrorf("batch error")
		b.pErr = pErr
		b.fillResults(ctx)

		require.Len(t, b.Results, 1)
		require.Error(t, b.Results[0].Err)
		require.Contains(t, b.Results[0].Err.Error(), "batch error")
	})
}

type mockBulkSource struct {
	data []struct {
		key   roachpb.Key
		value []byte
	}
}

func (m *mockBulkSource) Len() int {
	return len(m.data)
}

func (m *mockBulkSource) Iter() BulkSourceIterator[[]byte] {
	return &mockBulkSourceIterator{data: m.data, index: -1}
}

type mockBulkSourceIterator struct {
	data []struct {
		key   roachpb.Key
		value []byte
	}
	index int
}

func (m *mockBulkSourceIterator) Next() (roachpb.Key, []byte) {
	m.index++
	return m.data[m.index].key, m.data[m.index].value
}

func TestBatch_PutBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	source := &mockBulkSource{
		data: []struct {
			key   roachpb.Key
			value []byte
		}{
			{roachpb.Key("key1"), []byte("value1")},
			{roachpb.Key("key2"), []byte("value2")},
		},
	}

	b.PutBytes(source)

	require.Len(t, b.reqs, 2)
	require.Len(t, b.Results, 1)
	require.Equal(t, 2, b.Results[0].calls)

	putReq1 := b.reqs[0].GetInner().(*kvpb.PutRequest)
	putReq2 := b.reqs[1].GetInner().(*kvpb.PutRequest)

	require.Equal(t, roachpb.Key("key1"), putReq1.Key)
	val1, err := putReq1.Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val1)
	require.Equal(t, roachpb.Key("key2"), putReq2.Key)
	val2, err := putReq2.Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), val2)
}

func TestBatch_CPutBytesEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	source := &mockBulkSource{
		data: []struct {
			key   roachpb.Key
			value []byte
		}{
			{roachpb.Key("key1"), []byte("value1")},
		},
	}

	b.CPutBytesEmpty(source)

	require.Len(t, b.reqs, 1)
	cputReq := b.reqs[0].GetInner().(*kvpb.ConditionalPutRequest)
	require.Equal(t, roachpb.Key("key1"), cputReq.Key)
	require.False(t, cputReq.AllowIfDoesNotExist)
	require.Nil(t, cputReq.ExpBytes)
}

func TestBatch_invalidKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := &Batch{}
	b.Get(42)

	require.Len(t, b.Results, 1)
	require.Error(t, b.Results[0].Err)
	require.Contains(t, b.Results[0].Err.Error(), "unable to marshal key")
}

type mockValueBulkSource struct {
	data []struct {
		key   roachpb.Key
		value roachpb.Value
	}
}

func (m *mockValueBulkSource) Len() int {
	return len(m.data)
}

func (m *mockValueBulkSource) Iter() BulkSourceIterator[roachpb.Value] {
	return &mockValueBulkSourceIterator{data: m.data, index: -1}
}

type mockValueBulkSourceIterator struct {
	data []struct {
		key   roachpb.Key
		value roachpb.Value
	}
	index int
}

func (m *mockValueBulkSourceIterator) Next() (roachpb.Key, roachpb.Value) {
	m.index++
	return m.data[m.index].key, m.data[m.index].value
}

func TestBatch_RawAndNonRawMixing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("raw operations first", func(t *testing.T) {
		b := &Batch{}
		b.AddRawRequest(kvpb.NewGet(roachpb.Key("key")))
		require.True(t, b.raw)

		b.Get("key2")
		require.Len(t, b.Results, 2)
		require.Error(t, b.Results[1].Err)
		require.Contains(t, b.Results[1].Err.Error(), "must not use non-raw operations on a raw batch")
	})

	t.Run("non-raw operations first", func(t *testing.T) {
		b := &Batch{}
		b.Get("key1")
		require.False(t, b.raw)

		b.AddRawRequest(kvpb.NewGet(roachpb.Key("key2")))
		require.True(t, b.raw)
	})
}
