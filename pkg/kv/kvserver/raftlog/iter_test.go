// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package raftlog

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func ents(inds ...uint64) []raftpb.Entry {
	sl := make([]raftpb.Entry, 0, len(inds))
	for _, ind := range inds {
		cmd := kvserverpb.RaftCommand{
			MaxLeaseIndex: kvpb.LeaseAppliedIndex(ind), // just to have something nontrivial in here
		}
		b, err := protoutil.Marshal(&cmd)
		if err != nil {
			panic(err)
		}

		cmdID := kvserverbase.CmdIDKey(fmt.Sprintf("%8d", ind%100000000))

		var data []byte
		typ := raftpb.EntryType(ind % 3)
		switch typ {
		case raftpb.EntryNormal:
			enc := EntryEncodingStandardWithAC
			if ind%2 == 0 {
				enc = EntryEncodingSideloadedWithAC
			}
			data = EncodeCommandBytes(enc, cmdID, b, 0 /* pri */)
		case raftpb.EntryConfChangeV2:
			c := kvserverpb.ConfChangeContext{
				CommandID: string(cmdID),
				Payload:   b,
			}
			ccContext, err := protoutil.Marshal(&c)
			if err != nil {
				panic(err)
			}

			var cc raftpb.ConfChangeV2
			cc.Context = ccContext
			data, err = protoutil.Marshal(&cc)
			if err != nil {
				panic(err)
			}
		case raftpb.EntryConfChange:
			c := kvserverpb.ConfChangeContext{
				CommandID: string(cmdID),
				Payload:   b,
			}
			ccContext, err := protoutil.Marshal(&c)
			if err != nil {
				panic(err)
			}
			var cc raftpb.ConfChange
			cc.Context = ccContext
			data, err = protoutil.Marshal(&cc)
			if err != nil {
				panic(err)
			}
		default:
			panic(typ)
		}
		sl = append(sl, raftpb.Entry{
			Term:  100 + ind, // overflow ok
			Index: ind,
			Type:  typ,
			Data:  data,
		})
	}
	return sl
}

// modelIter is an intentionally dumb model version of the iterator that should
// produce the same seek/iter behavior as the real iterator and can thus
// validate its behaviors.
type modelIter struct {
	idx  int
	ents []raftpb.Entry
	hi   kvpb.RaftIndex
}

func (it *modelIter) load() (raftpb.Entry, error) {
	// Only called when on valid position.
	return it.ents[it.idx], nil
}

func (it *modelIter) check() error {
	// Only called when on valid position.
	_, err := it.load()
	return err
}

func (it *modelIter) SeekGE(lo kvpb.RaftIndex) (bool, error) {
	for {
		if it.idx >= len(it.ents) || (it.hi > 0 && kvpb.RaftIndex(it.ents[it.idx].Index) >= it.hi) {
			return false, nil
		}
		if ind := kvpb.RaftIndex(it.ents[it.idx].Index); ind >= lo && (it.hi == 0 || ind < it.hi) {
			if err := it.check(); err != nil {
				return false, err
			}
			return true, nil
		}
		it.idx++
	}
}

func (it *modelIter) Next() (bool, error) {
	it.idx++
	if it.idx >= len(it.ents) {
		return false, nil
	}
	if it.hi > 0 && kvpb.RaftIndex(it.ents[it.idx].Index) >= it.hi {
		return false, nil
	}
	err := it.check()
	return err == nil, err
}

func (it *modelIter) Entry() raftpb.Entry {
	e, err := it.load()
	if err != nil {
		panic(err) // bug in modelIter
	}
	return e
}

func TestIteratorEmptyLog(t *testing.T) {
	defer log.Scope(t).Close(t)

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	for _, hi := range []kvpb.RaftIndex{0, 1} {
		it, err := NewIterator(context.Background(), rangeID, eng, IterOptions{Hi: hi})
		require.NoError(t, err)
		ok, err := it.SeekGE(0)
		it.Close()
		require.NoError(t, err)
		require.False(t, ok)
	}
}

// TestIterator sets up a few raft logs and iterates all conceivable chunks of
// them with both the real and a model iterator, comparing the results.
func TestIterator(t *testing.T) {
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name string
		ents []raftpb.Entry
	}{
		{
			ents: ents(5),
		},
		{
			ents: ents(5, 6),
		},
		{
			ents: ents(5, 6, 7),
		},
		{
			ents: ents(1, 2, 3),
		},
		{
			ents: ents(math.MaxUint64-1, math.MaxUint64),
		},
	} {
		indToName := func(ind kvpb.RaftIndex) string {
			if ind > math.MaxUint64/2 {
				if ind == math.MaxUint64 {
					return "max"
				}
				return fmt.Sprintf("max-%d", math.MaxUint64-ind)
			}
			return fmt.Sprint(ind)
		}
		var inds []string
		for _, ent := range tc.ents {
			inds = append(inds, indToName(kvpb.RaftIndex(ent.Index)))
		}
		if len(inds) == 0 {
			inds = []string{"empty"}
		}
		t.Run("log="+strings.Join(inds, ","), func(t *testing.T) {
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			const rangeID = 1

			// Populate raft log.
			for _, ent := range tc.ents {
				e, err := NewEntry(ent)
				require.NoError(t, err)
				metaB, err := e.ToRawBytes()
				require.NoError(t, err)
				require.NoError(t, eng.PutUnversioned(keys.RaftLogKey(rangeID, kvpb.RaftIndex(ent.Index)), metaB))
			}

			// Rather than handcrafting some invocations, just run all possible ones
			// and verify against our expectations. There's no need to hard-code them
			// since they're so simple to express.
			var fi kvpb.RaftIndex // firstIndex
			var li kvpb.RaftIndex // lastIndex
			if n := len(tc.ents); n > 0 {
				fi = kvpb.RaftIndex(tc.ents[0].Index)
				li = kvpb.RaftIndex(tc.ents[n-1].Index)
			} else {
				// Make sure we do some bogus scans on the empty log as well.
				fi = 1
				li = 2
			}
			for lo := fi - 1; lo-1 < li; lo++ {
				// If (lo,hi)==(max,max) then in the next iteration hi==0, followed by
				// hi==1 (which gets mapped to hi=0 as well since 1-2==max); the same
				// happens with other examples that wrap around. Adjust `li` a bit in
				// these cases to avoid the duplicate; we want to see `hi` approach `max`,
				// then hit it, then move to zero, then stop. Without this, we'd see another
				// zero at the end.
				li := li
				if li > math.MaxUint64-1 {
					li = math.MaxUint64 - 1
				}
				for hi := lo - 1; hi-3 < li; hi++ {
					if hi-2 == li {
						// As the last case, make `hi` unlimited.
						hi = 0
					}
					t.Run(fmt.Sprintf("lo=%s,hi=%s", indToName(lo), indToName(hi)), func(t *testing.T) {
						it, err := NewIterator(context.Background(), rangeID, eng, IterOptions{Hi: hi})
						require.NoError(t, err)
						sl, err := consumeIter(it, lo)
						it.Close()
						require.NoError(t, err)

						it2 := &modelIter{ents: tc.ents, hi: hi}
						sl2, err := consumeIter(it2, lo)
						require.NoError(t, err)

						require.Equal(t, sl2, sl)

						// Empty [lo,hi) should return no entries.
						// Note that hi==0 means hi==MaxUint64.
						if hi > 0 && lo >= hi {
							require.Zero(t, sl)
						}

						t.Log(sl)
					})
				}
			}
		})
	}
}

type iter interface {
	SeekGE(idx kvpb.RaftIndex) (bool, error)
	Next() (bool, error)
	Entry() raftpb.Entry
}

func consumeIter(it iter, lo kvpb.RaftIndex) ([]uint64, error) {
	var sl []uint64

	ok, err := it.SeekGE(lo)
	if err != nil {
		return nil, err
	}
	for ; ok; ok, err = it.Next() {
		sl = append(sl, it.Entry().Index)
	}
	if err != nil {
		return nil, err
	}
	return sl, nil
}
