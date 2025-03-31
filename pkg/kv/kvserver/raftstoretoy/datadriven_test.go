// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/rspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestRaftStoreToy is the main entry point for our datadriven tests.
// It demonstrates the core functionality of the Raft storage design through
// simple, educational examples.
func TestRaftStoreToy(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		c := &rspb.CodecV2{}
		llEng := &mockEngine{}
		env := Env{
			llEng:  llEng,
			logEng: &LogEngine{c: c, e: llEng},
			smEng:  nil,
		}

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			env.Handle(t, d)
			return env.Output()
		})
	})
}

type Env struct {
	c      rspb.CodecV2
	llEng  *mockEngine
	logEng *LogEngine
	smEng  SMEngine

	out strings.Builder
}

func (e *Env) logf(format string, args ...interface{}) {
	e.out.WriteString(strings.TrimSpace(fmt.Sprintf(format, args...)) + "\n")
}

func (e *Env) Output() string {
	return e.out.String()
}

func (e *Env) Handle(t *testing.T, d *datadriven.TestData) {
	e.out.Reset()
	defer func() {
		if e.out.Len() == 0 {
			e.logf("ok")
		}
	}()

	ctx := context.Background()
	switch d.Cmd {
	case "enc":
		var kind string
		var rangeID int64
		var logID int64
		var raftIdx uint64
		d.ScanArgs(t, "kind", &kind)
		d.ScanArgs(t, "rid", &rangeID)
		d.ScanArgs(t, "lid", &logID)
		d.MaybeScanArgs(t, "ridx", &raftIdx)
		sl := e.c.Encode(nil, rspb.KeyKindByString[kind], rspb.RangeID(rangeID), rspb.LogID(logID),
			rspb.RaftIndex(raftIdx))
		e.logf("%x", sl)
		sl, kk, rid, lid, ridx, err := e.c.Decode(sl)
		if err != nil {
			e.logf("%s", err)
			break
		}
		require.EqualValues(t, rspb.KeyKindByString[kind], kk)
		require.EqualValues(t, rangeID, rid)
		require.EqualValues(t, logID, lid)
		require.EqualValues(t, logID, lid)
		require.EqualValues(t, raftIdx, ridx)
		require.Empty(t, sl)
	case "ll-eng":
		var sync bool
		d.MaybeScanArgs(t, "sync", &sync)
		b := e.llEng.NewBatch()
		dump := false
		for _, line := range strings.Split(d.Input, "\n") {
			fs := strings.Fields(line)
			var cmd, k, v string
			switch len(fs) {
			case 3:
				cmd, k, v = fs[0], fs[1], fs[2]
			case 2:
				cmd, k = fs[0], fs[1]
			case 1:
				cmd = fs[0]
			}
			switch cmd {
			case "put":
				dump = true
				b.Put(ctx, roachpb.Key(k), []byte(v))
			case "get":
				v, err := e.llEng.Get(roachpb.Key(k))
				if err != nil {
					e.logf("%s", err)
					return
				}
				if v == nil {
					v = []byte(tombstone)
				}
				e.logf("%s: %s\n", k, v)
			case "del":
				dump = true
				b.Del(ctx, roachpb.Key(k))
			case "flush":
				dump = true
				if err := e.llEng.Flush(); err != nil {
					e.logf("%s", err)
					return
				}
			case "crash":
				e.llEng.Crash()
				dump = true
			case "":
				// Syncing empty batch.
				dump = true
			default:
				t.Fatalf("unknown command: %s", cmd)
			}
		}
		if err := b.Commit(sync); err != nil {
			e.logf("%s", err)
			return
		}
		if dump {
			require.NoError(t, e.llEng.Dump(&e.out))
		}
	case "create":
		var rangeID int64
		var replID int64
		d.ScanArgs(t, "rid", &rangeID)
		d.ScanArgs(t, "replid", &replID)
		op, err := e.Create(CreateRequest{
			RangeID:   rspb.RangeID(rangeID),
			ReplicaID: rspb.ReplicaID(replID),
		})
		if err != nil {
			e.logf("%s", err)
			return
		}
		e.logf("%s: %s", op.WAGIndex, op.LogID)
	default:
		t.Fatalf("unknown command: %s", d.Cmd)
	}
}

func (e *Env) Create(req CreateRequest) (rspb.CreateOp, error) {
	b := e.llEng.NewSyncedBatch()
	op, err := e.logEng.Create(context.Background(), b, req)
	if err != nil {
		return rspb.CreateOp{}, err
	}
	if err := b.Commit(); err != nil {
		return rspb.CreateOp{}, err
	}
	return op, nil
}
