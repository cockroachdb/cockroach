// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Putter is an interface for layering functionality on the path from SQL
// encoding logic to kv.Batch.
type Putter interface {
	CPut(key, value interface{}, expValue []byte)
	CPutWithOriginTimestamp(key, value interface{}, expValue []byte, ts hlc.Timestamp, shouldWinTie bool)
	Put(key, value interface{})
	PutMustAcquireExclusiveLock(key, value interface{})
	Del(key ...interface{})

	CPutBytesEmpty(kys []roachpb.Key, values [][]byte)
	CPutValuesEmpty(kys []roachpb.Key, values []roachpb.Value)
	CPutTuplesEmpty(kys []roachpb.Key, values [][]byte)
	PutBytes(kys []roachpb.Key, values [][]byte)
	PutTuples(kys []roachpb.Key, values [][]byte)
}

// TracePutter logs all requests, ie implements kv trace.
type TracePutter struct {
	Putter Putter
	Ctx    context.Context
}

var _ Putter = &TracePutter{}

func (t *TracePutter) CPut(key, value interface{}, expValue []byte) {
	log.VEventfDepth(t.Ctx, 1, 2, "CPut %v -> %v", key, value)
	t.Putter.CPut(key, value, expValue)
}

func (t *TracePutter) CPutWithOriginTimestamp(
	key, value interface{}, expValue []byte, ts hlc.Timestamp, shouldWinTie bool,
) {
	log.VEventfDepth(t.Ctx, 1, 2, "CPutWithOriginTimestamp %v -> %v @ %v", key, value, ts)
	t.Putter.CPutWithOriginTimestamp(key, value, expValue, ts, shouldWinTie)
}

func (t *TracePutter) Put(key, value interface{}) {
	log.VEventfDepth(t.Ctx, 1, 2, "Put %v -> %v", key, value)
	t.Putter.Put(key, value)
}

func (t *TracePutter) PutMustAcquireExclusiveLock(key, value interface{}) {
	log.VEventfDepth(t.Ctx, 1, 2, "Put (locking) %v -> %v", key, value)
	t.Putter.PutMustAcquireExclusiveLock(key, value)
}

func (t *TracePutter) Del(key ...interface{}) {
	log.VEventfDepth(t.Ctx, 1, 2, "Del %v", key...)
	t.Putter.Del(key...)
}

func (t *TracePutter) CPutBytesEmpty(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if len(k) == 0 {
			continue
		}
		var v roachpb.Value
		v.SetBytes(values[i])
		log.VEventfDepth(t.Ctx, 1, 2, "CPut %s -> %s", k, v.PrettyPrint())
	}
	t.Putter.CPutBytesEmpty(kys, values)
}

func (t *TracePutter) CPutValuesEmpty(kys []roachpb.Key, values []roachpb.Value) {
	for i, k := range kys {
		if len(k) == 0 {
			continue
		}
		log.VEventfDepth(t.Ctx, 1, 2, "CPut %s -> %s", k, values[i].PrettyPrint())
	}
	t.Putter.CPutValuesEmpty(kys, values)
}

func (t *TracePutter) CPutTuplesEmpty(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if len(k) == 0 {
			continue
		}
		var v roachpb.Value
		v.SetTuple(values[i])
		log.VEventfDepth(t.Ctx, 1, 2, "CPut %s -> %s", k, v.PrettyPrint())
	}
	t.Putter.CPutTuplesEmpty(kys, values)
}

func (t *TracePutter) PutBytes(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if len(k) == 0 {
			continue
		}
		var v roachpb.Value
		v.SetBytes(values[i])
		log.VEventfDepth(t.Ctx, 1, 2, "Put %s -> %s", k, v.PrettyPrint())
	}
	t.Putter.PutBytes(kys, values)
}

func (t *TracePutter) PutTuples(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if len(k) == 0 {
			continue
		}
		var v roachpb.Value
		v.SetTuple(values[i])
		log.VEventfDepth(t.Ctx, 1, 2, "Put %s -> %s", k, v.PrettyPrint())
	}
	t.Putter.PutTuples(kys, values)
}

type KVBytes struct {
	Keys   []roachpb.Key
	Values [][]byte
}

var _ sort.Interface = &KVBytes{}

func (k *KVBytes) Len() int {
	return len(k.Keys)
}

func (k *KVBytes) Less(i, j int) bool {
	return bytes.Compare(k.Keys[i], k.Keys[j]) < 0
}

func (k *KVBytes) Swap(i, j int) {
	k.Keys[i], k.Keys[j] = k.Keys[j], k.Keys[i]
	k.Values[i], k.Values[j] = k.Values[j], k.Values[i]
}

type KVVals struct {
	Keys   []roachpb.Key
	Values []roachpb.Value
}

var _ sort.Interface = &KVVals{}

func (k *KVVals) Len() int {
	return len(k.Keys)
}

func (k *KVVals) Less(i, j int) bool {
	return bytes.Compare(k.Keys[i], k.Keys[j]) < 0
}

func (k *KVVals) Swap(i, j int) {
	k.Keys[i], k.Keys[j] = k.Keys[j], k.Keys[i]
	k.Values[i], k.Values[j] = k.Values[j], k.Values[i]
}

type SortingPutter struct {
	Putter Putter
}

var _ Putter = &SortingPutter{}

func (s *SortingPutter) CPut(key, value interface{}, expValue []byte) {
	s.Putter.CPut(key, value, expValue)
}

func (s *SortingPutter) CPutWithOriginTimestamp(
	key, value interface{}, expValue []byte, ts hlc.Timestamp, shouldWinTie bool,
) {
	s.Putter.CPutWithOriginTimestamp(key, value, expValue, ts, shouldWinTie)
}

func (s *SortingPutter) Put(key, value interface{}) {
	s.Putter.Put(key, value)
}

func (s *SortingPutter) PutMustAcquireExclusiveLock(key, value interface{}) {
	s.Putter.PutMustAcquireExclusiveLock(key, value)
}

func (s *SortingPutter) Del(key ...interface{}) {
	s.Putter.Del(key...)
}

func (s *SortingPutter) CPutBytesEmpty(kys []roachpb.Key, values [][]byte) {
	kvs := KVBytes{Keys: kys, Values: values}
	sort.Sort(&kvs)
	s.Putter.CPutBytesEmpty(kvs.Keys, kvs.Values)
}

func (s *SortingPutter) CPutValuesEmpty(kys []roachpb.Key, values []roachpb.Value) {
	kvs := KVVals{Keys: kys, Values: values}
	sort.Sort(&kvs)
	s.Putter.CPutValuesEmpty(kvs.Keys, kvs.Values)
}

func (s *SortingPutter) CPutTuplesEmpty(kys []roachpb.Key, values [][]byte) {
	kvs := KVBytes{Keys: kys, Values: values}
	sort.Sort(&kvs)
	s.Putter.CPutTuplesEmpty(kvs.Keys, kvs.Values)
}

func (s *SortingPutter) PutBytes(kys []roachpb.Key, values [][]byte) {
	kvs := KVBytes{Keys: kys, Values: values}
	sort.Sort(&kvs)
	s.Putter.PutBytes(kvs.Keys, kvs.Values)
}

func (s *SortingPutter) PutTuples(kys []roachpb.Key, values [][]byte) {
	kvs := KVBytes{Keys: kys, Values: values}
	sort.Sort(&kvs)
	s.Putter.PutTuples(kvs.Keys, kvs.Values)
}

type kvSparseSliceBulkSource[T kv.GValue] struct {
	keys   []roachpb.Key
	values []T
}

var _ kv.BulkSource[[]byte] = &kvSparseSliceBulkSource[[]byte]{}

func (k *kvSparseSliceBulkSource[T]) Len() int {
	cnt := 0
	for _, k := range k.keys {
		if len(k) > 0 {
			cnt++
		}
	}
	return cnt
}

type sliceIterator[T kv.GValue] struct {
	s      *kvSparseSliceBulkSource[T]
	cursor int
}

var _ kv.BulkSourceIterator[[]byte] = &sliceIterator[[]byte]{}

func (k *kvSparseSliceBulkSource[T]) Iter() kv.BulkSourceIterator[T] {
	return &sliceIterator[T]{k, 0}
}

func (s *sliceIterator[T]) Next() (roachpb.Key, T) {
	for {
		k, v := s.s.keys[s.cursor], s.s.values[s.cursor]
		s.cursor++
		if len(k) > 0 {
			return k, v
		}
	}
}

// KVBatchAdapter implements Putter interface and adapts it to kv.Batch API.
type KVBatchAdapter struct {
	Batch *kv.Batch
}

var _ Putter = &KVBatchAdapter{}

func (k *KVBatchAdapter) CPutWithOriginTimestamp(
	key, value interface{}, expValue []byte, originTimestamp hlc.Timestamp, shouldWinTie bool,
) {
	k.Batch.CPutWithOriginTimestamp(key, value, expValue, originTimestamp, shouldWinTie)
}

func (k *KVBatchAdapter) CPut(key, value interface{}, expValue []byte) {
	k.Batch.CPut(key, value, expValue)
}

func (k *KVBatchAdapter) Put(key, value interface{}) {
	k.Batch.Put(key, value)
}

func (k *KVBatchAdapter) PutMustAcquireExclusiveLock(key, value interface{}) {
	k.Batch.PutMustAcquireExclusiveLock(key, value)
}

func (k *KVBatchAdapter) Del(key ...interface{}) {
	k.Batch.Del(key...)
}

func (k *KVBatchAdapter) CPutBytesEmpty(kys []roachpb.Key, values [][]byte) {
	k.Batch.CPutBytesEmpty(&kvSparseSliceBulkSource[[]byte]{kys, values})
}

func (k *KVBatchAdapter) CPutValuesEmpty(kys []roachpb.Key, values []roachpb.Value) {
	k.Batch.CPutValuesEmpty(&kvSparseSliceBulkSource[roachpb.Value]{kys, values})
}

func (k *KVBatchAdapter) CPutTuplesEmpty(kys []roachpb.Key, values [][]byte) {
	k.Batch.CPutTuplesEmpty(&kvSparseSliceBulkSource[[]byte]{kys, values})
}

func (k *KVBatchAdapter) PutBytes(kys []roachpb.Key, values [][]byte) {
	k.Batch.PutBytes(&kvSparseSliceBulkSource[[]byte]{kys, values})
}

func (k *KVBatchAdapter) PutTuples(kys []roachpb.Key, values [][]byte) {
	k.Batch.PutTuples(&kvSparseSliceBulkSource[[]byte]{kys, values})
}
