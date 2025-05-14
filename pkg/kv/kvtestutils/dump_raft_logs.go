// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtestutils

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

type RaftLogDumper struct {
	Dir    string
	logged bool
}

func (d *RaftLogDumper) Dump(
	t interface {
		Logf(format string, args ...interface{})
	},
	reader storage.Reader,
	storeID roachpb.StoreID,
	rangeID roachpb.RangeID,
) {
	if !d.logged {
		t.Logf("dumping raft logs to %s", d.Dir)
		d.logged = true
	}
	if err := d.dumpOne(reader, storeID, rangeID); err != nil {
		t.Logf("error dumping s%dr%d: %s", storeID, rangeID, err)
	}
}

func (d *RaftLogDumper) dumpOne(
	reader storage.Reader, storeID roachpb.StoreID, rangeID roachpb.RangeID,
) error {
	if err := os.MkdirAll(d.Dir, 0755); err != nil {
		return err
	}
	path := filepath.Join(d.Dir, "raftlog_s"+storeID.String()+"r"+rangeID.String()+".txt")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := print.DumpRaftLog(f, reader, rangeID); err != nil {
		_, _ = fmt.Fprintf(f, "error on %s: %s", rangeID, err)
	}
	return f.Close()
}
