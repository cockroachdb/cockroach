// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package print

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/gogo/protobuf/proto"
)

// DumpRaftLog writes a debugging text representation of the raft log into the
// provided writer.
func DumpRaftLog(w io.Writer, reader storage.Reader, rangeID roachpb.RangeID) error {
	ctx := context.Background()
	put := func(format string, args ...interface{}) {
		_, _ = fmt.Fprintf(w, format, args...)
		_, _ = fmt.Fprintln(w)
	}

	return raftlog.Visit(ctx, reader, rangeID, 0, math.MaxUint64, func(ent raftpb.Entry) error {
		put("****** index %d ******", ent.Index)
		e, err := raftlog.NewEntry(ent)
		if err != nil {
			put("%s", err)
			return nil // continue
		}
		defer e.Release()
		wb := e.Cmd.WriteBatch
		e.Cmd.WriteBatch = nil

		// NB: I spent way too much time trying to find a way to output the
		// struct recursively but while omitting zero values. This is the
		// closest I got without putting in significant amounts of work (like
		// adjusting dozens of gogoproto moretags or writing custom
		// formatters).
		put("RaftCommand: %+v", proto.CompactTextString(&e.Cmd))

		s, err := DecodeWriteBatch(wb.GetData())
		if err != nil {
			put("%s", err)
			return nil // continue
		}
		put("%s", strings.TrimSpace(s))
		return nil
	})
}
