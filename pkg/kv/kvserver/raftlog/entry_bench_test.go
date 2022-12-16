// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package raftlog

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func BenchmarkNewEntry(b *testing.B) {
	ent, metaB := mkBenchEnt(b)
	b.ResetTimer()
	for _, fromRawValue := range []bool{false, true} {
		for _, release := range []bool{false, true} {
			b.Run(fmt.Sprintf("fromRawValue=%t,release=%t", fromRawValue, release), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var e *Entry
					var err error
					if fromRawValue {
						e, err = NewEntryFromRawValue(metaB)
					} else {
						e, err = NewEntry(ent)
					}
					if err != nil {
						b.Fatal(err)
					}
					if release {
						e.Release()
					}
				}
			})
		}
	}
}

func BenchmarkMarshalUnmarshal(b *testing.B) {
	const kb = 1 << 10
	for _, bytes := range []int64{
		0,
		128,
		512,
		1 * kb,
		8 * kb,
		16 * kb,
		32 * kb,
		64 * kb,
		128 * kb,
		1 * kb * kb, // 1mb
	} {
		for _, typ := range []string{
			"slice",
			"marshal",
			"unmarshal-no-reuse",
			"unmarshal-reuse",
		} {
			b.Run(fmt.Sprintf("type=%s,size=%s", typ, humanizeutil.IBytes(bytes)), func(b *testing.B) {
				src := make([]byte, bytes)
				_, _ = rand.Read(src)

				var work func()
				if typ == "slice" {
					dst := make([]byte, bytes)
					work = func() {
						copy(dst, src)
					}
				} else {
					cmd := &kvserverpb.RaftCommand{}
					cmd.WriteBatch = &kvserverpb.WriteBatch{Data: src}
					dst := make([]byte, cmd.Size())
					marshal := func() {
						if _, err := cmd.MarshalToSizedBuffer(dst); err != nil {
							b.Fatal(err)
						}
					}
					if typ == "marshal" {
						work = marshal
					} else {
						marshal() // populate dst
						var cmd2 kvserverpb.RaftCommand
						reset := cmd2.Reset
						if typ == "unmarshal-reuse" {
							reset = func() {
								if cmd2.WriteBatch != nil {
									// Reuse as much memory as possible. Should not need to allocate.
									cmd2.WriteBatch.Data = cmd2.WriteBatch.Data[:0]
								}
							}
						}
						work = func() {
							reset()
							if err := cmd2.Unmarshal(dst); err != nil {
								b.Fatal(err)
							}
						}
					}
				}

				b.ReportAllocs()
				tBegin := timeutil.Now()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					work()
				}
				b.StopTimer()
				dur := timeutil.Since(tBegin)
				b.ReportMetric(float64(int64(b.N)*bytes)/(dur.Seconds()*kb*kb), "mb/s")
			})
		}
	}
}
