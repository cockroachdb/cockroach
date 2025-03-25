// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/logpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type LowLevelEngine struct {
	eng storage.Engine
}

type LowLevelBatch struct {
	b storage.Batch
}

func (llb *LowLevelBatch) Put(ctx context.Context, key roachpb.Key, value []byte) error {
	var v roachpb.Value
	v.SetBytes(value)
	_, err := storage.MVCCPut(ctx, llb.b, key, hlc.Timestamp{}, v, storage.MVCCWriteOptions{})
	return err
}

func (llb *LowLevelBatch) Clear(ctx context.Context, key roachpb.Key) error {
	_, _, err := storage.MVCCDelete(ctx, llb.b, key, hlc.Timestamp{}, storage.MVCCWriteOptions{})
	return err
}

func (llb *LowLevelBatch) Commit(sync bool) error {
	return llb.b.Commit(sync)
}

func (llb *LowLevelBatch) Close() {
	llb.b.Close()
}

func (lle *LowLevelEngine) NewBatch() *LowLevelBatch {
	return &LowLevelBatch{b: lle.eng.NewBatch()}
}

func (lle *LowLevelEngine) Flush() error {
	return lle.eng.Flush()
}

func (lle *LowLevelEngine) Dump(w io.Writer, enc Encoding) error {
	type strMVCCKey struct {
		key string
		ts  hlc.Timestamp
	}
	type val struct {
		durStd, durGD []byte
	}
	seen := map[strMVCCKey]val{}
	for _, dur := range []storage.DurabilityRequirement{
		storage.StandardDurability, storage.GuaranteedDurability,
	} {
		r := lle.eng.NewReader(dur)
		err := r.MVCCIterate(context.Background(),
			roachpb.LocalMax, roachpb.KeyMax,
			storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsAndRanges, fs.UnknownReadCategory,
			func(value storage.MVCCKeyValue, stack storage.MVCCRangeKeyStack) error {
				if !stack.IsEmpty() {
					// We don't need this in this toy.
					return errors.New("unimplemented: range keys")
				}
				if !value.Key.Timestamp.IsEmpty() {
					// We don't need to bring MVCC into this toy.
					return errors.New("unimplemented: MVCC timestamps")
				}
				sKey := strMVCCKey{
					key: string(value.Key.Key),
					ts:  value.Key.Timestamp,
				}
				if _, ok := seen[sKey]; !ok {
					seen[sKey] = val{}
				}
				v := seen[sKey]

				switch dur {
				case storage.StandardDurability:
					v.durStd = value.Value
				case storage.GuaranteedDurability:
					v.durGD = value.Value
				default:
					panic("unknown durability")
				}
				seen[sKey] = v
				return nil
			})
		r.Close()
		if err != nil {
			return err
		}
	}

	type flat struct {
		k strMVCCKey
		v val
	}
	var sl []flat
	for k, v := range seen {
		sl = append(sl, flat{k, v})
	}
	sort.Slice(sl, func(i, j int) bool {
		if sl[i].k.key == sl[j].k.key {
			if sl[i].k.ts == sl[j].k.ts {
				iStdSet, jStdSet := sl[i].v.durStd != nil, sl[j].v.durStd != nil
				iGDSet, jGDSet := sl[i].v.durGD != nil, sl[j].v.durGD != nil
				if iStdSet == jStdSet {
					return iGDSet != jGDSet
				}
				if jStdSet {
					return true
				}
				return false
			}
			return sl[i].k.ts.Less(sl[j].k.ts)
		}
		if sl[i].k.key < sl[j].k.key {
			return true
		}
		return false
	})

	for _, f := range sl {
		obj, err := enc.Decode([]byte(f.k.key), nil)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(w, "%+v", obj)
		if !f.k.ts.IsEmpty() {
			_, _ = fmt.Fprint(w, " ", f.k.ts)
		}
		_, _ = fmt.Fprint(w, " ->")
		decor := "*"
		if bytes.Equal(f.v.durStd, f.v.durGD) {
			decor = ""
			f.v.durGD = nil
		}
		_, _ = fmt.Fprint(w, " ", string(decInlineVal(f.v.durStd)), decor)
		if f.v.durGD != nil {
			_, _ = fmt.Fprint(w, " ", string(decInlineVal(f.v.durGD)))
		}
		_, _ = fmt.Fprintln(w)
	}
	return nil
}

func decInlineVal(v []byte) []byte {
	var meta enginepb.MVCCMetadata
	if err := meta.Unmarshal(v); err != nil {
		return []byte(err.Error())
	}
	val := roachpb.Value{RawBytes: meta.RawBytes}
	result, err := val.GetBytes()
	if err != nil {
		return []byte(err.Error())
	}
	return result
}

func NewLowLevelEngine() *LowLevelEngine {
	return &LowLevelEngine{eng: storage.NewDefaultInMemForTesting()}
}

type llLogEngine struct {
	enc Encoding
	e   *LowLevelEngine

	buf []byte // scratch buf
}

func (llle *llLogEngine) Append(ctx context.Context, id FullLogID, entry LogEntry) error {
	//TODO implement me
	panic("implement me")
}

func (llle *llLogEngine) Create(
	ctx context.Context, req CreateRequest,
) (FullLogID, WAGIndex, error) {
	b := llle.e.NewBatch()
	defer b.Close()

	lid := logpb.LogID(1) // TODO(tbg): allocate
	// wix := WAGIndex(123)  // TODO(tbg): allocate

	op := CreateOp{
		ID: FullLogID{
			RangeID:   req.RangeID,
			ReplicaID: req.ReplicaID,
			LogID:     lid,
		},
	}

	llle.enc.Encode(llle.buf[:0], op)
	err := b.Put(ctx, MakeKey(req.RangeID, req.ReplicaID), []byte("hi"))
	return FullLogID{}, 0, err
}

func (llle *llLogEngine) get(ctx context.Context, k Key) ([]byte, error) {
	r := llle.e.eng.NewReader(storage.StandardDurability)
	defer r.Close()
	res, err := storage.MVCCGet(ctx, r, k.Encode(), hlc.Timestamp{}, storage.MVCCGetOptions{})
	if err != nil {
		return nil, err
	}
	if !res.Value.IsPresent() {
		return nil, nil
	}
	return res.Value.GetBytes()
}

func (llle *llLogEngine) Destroy(ctx context.Context, id FullLogID, req Destroy) (WAGIndex, error) {
	//TODO implement me
	panic("implement me")
}
