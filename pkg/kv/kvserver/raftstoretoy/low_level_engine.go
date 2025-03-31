// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/rscodec"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

type LLBatch interface {
	Put(ctx context.Context, key roachpb.Key, value []byte)
	Del(ctx context.Context, key roachpb.Key)
	Commit(sync bool) error
	Close()
}

type LLEngine interface {
	NewBatch() LLBatch
	Flush() error
	Dump(w io.Writer) error
}

type mockBatch struct {
	e  *mockEngine
	wb map[string]string
}

func (b *mockBatch) Put(ctx context.Context, key roachpb.Key, value []byte) {
	b.wb[string(key)] = string(value)
}

func (b *mockBatch) Del(ctx context.Context, key roachpb.Key) {
	b.wb[string(key)] = "\x00"
}

func (b *mockBatch) Commit(sync bool) error {
	b.e.commit(b.wb, sync)
	return nil
}

func (b *mockBatch) Close() {}

type mockEngine struct {
	vol map[string]string // in memtbl, not durable and not flushed
	dur map[string]string // in memtbl, durable but not flushed
	lsm map[string]string // LSM contents (flushed and durable)
}

var _ LLEngine = &mockEngine{}

func (e *mockEngine) NewBatch() LLBatch {
	return &mockBatch{
		e:  e,
		wb: make(map[string]string),
	}
}

func (e *mockEngine) commit(wb map[string]string, sync bool) {
	if e.dur == nil {
		e.dur = make(map[string]string)
	}
	if e.vol == nil {
		e.vol = make(map[string]string)
	}
	for k, v := range wb {
		e.vol[k] = v
	}

	if sync {
		for k, v := range e.vol {
			e.dur[k] = v
		}
		e.vol = nil
	}
}

func (e *mockEngine) Flush() error {
	e.commit(nil, true)
	if e.lsm == nil {
		e.lsm = make(map[string]string)
	}
	for k, v := range e.dur {
		e.lsm[k] = v
	}
	e.dur = nil
	for k, v := range e.lsm {
		if v == "\x00" {
			delete(e.lsm, k)
		}
	}
	return nil
}

func (e *mockEngine) Dump(w io.Writer) error {
	type val struct {
		vol, dur, lsm []byte
	}
	dst := map[string]val{}
	for k, v := range e.vol {
		dst[k] = val{vol: []byte(v)}
	}
	for k, v := range e.dur {
		ent := dst[k]
		ent.dur = []byte(v)
		dst[k] = ent
	}
	for k, v := range e.lsm {
		ent := dst[k]
		ent.lsm = []byte(v)
		dst[k] = ent
	}

	type flat struct {
		k roachpb.Key
		v val
	}
	var sl []flat
	for k, v := range dst {
		sl = append(sl, flat{roachpb.Key(k), v})
	}
	sort.Slice(sl, func(i, j int) bool {
		if sl[i].k.Equal(sl[j].k) {
			iStdSet, jStdSet := sl[i].v.dur != nil, sl[j].v.dur != nil
			iGDSet, jGDSet := sl[i].v.lsm != nil, sl[j].v.lsm != nil
			if iStdSet == jStdSet {
				return iGDSet != jGDSet
			}
			if jStdSet {
				return true
			}
			return false
		}
		if sl[i].k.Less(sl[j].k) {
			return true
		}
		return false
	})

	for _, f := range sl {
		_, _ = fmt.Fprintf(w, "%s ->", f.k)
		if f.v.vol != nil {
			_, _ = fmt.Fprintf(w, " %q%s", f.v.vol, "♱")
		}
		if f.v.dur != nil {
			_, _ = fmt.Fprintf(w, " %q%s", f.v.dur, "⚐")
		}
		if f.v.lsm != nil {
			_, _ = fmt.Fprintf(w, " %q", f.v.lsm)
		}
		_, _ = fmt.Fprintln(w)
	}
	return nil
}

type llLogEngine struct {
	c rscodec.Codec
	e LLEngine

	buf []byte // scratch buf
}

func (llle *llLogEngine) Append(ctx context.Context, id rscodec.FullLogID, entry LogEntry) error {
	//TODO implement me
	panic("implement me")
}

func (llle *llLogEngine) Create(
	ctx context.Context, req CreateRequest,
) (rscodec.FullLogID, WAGIndex, error) {
	b := llle.e.NewBatch()
	defer b.Close()

	lid := rscodec.LogID(1) // TODO(tbg): allocate
	// wix := WAGIndex(123)  // TODO(tbg): allocate

	op := CreateOp{
		ID: rscodec.FullLogID{
			RangeID:   req.RangeID,
			ReplicaID: req.ReplicaID,
			LogID:     lid,
		},
	}
	_ = op

	// llle.c.Encode(llle.buf[:0], op)
	err := errors.New("fixme")
	// err := b.Put(ctx, MakeKey(req.RangeID, req.ReplicaID), []byte("hi"))
	return rscodec.FullLogID{}, 0, err
}

//func (llle *llLogEngine) get(ctx context.Context, k Key) ([]byte, error) {
//	r := llle.e.eng.NewReader(storage.StandardDurability)
//	defer r.Close()
//	res, err := storage.MVCCGet(ctx, r, k.Encode(), hlc.Timestamp{}, storage.MVCCGetOptions{})
//	if err != nil {
//		return nil, err
//	}
//	if !res.Value.IsPresent() {
//		return nil, nil
//	}
//	return res.Value.GetBytes()
//}

func (llle *llLogEngine) Destroy(
	ctx context.Context, id rscodec.FullLogID, req Destroy,
) (WAGIndex, error) {
	//TODO implement me
	panic("implement me")
}
