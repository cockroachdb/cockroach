// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// MockBackupChain returns a chain of mock backup manifests that have spans and
// file spans suitable for checking coverage computations. Every 3rd inc backup
// introduces a span and drops a span. Incremental backups have half as many
// files as the base. Files spans are ordered by start key but may overlap.
func MockBackupChain(length, spans, baseFiles int, r *rand.Rand) []BackupManifest {
	backups := make([]BackupManifest, length)
	ts := hlc.Timestamp{WallTime: time.Second.Nanoseconds()}
	for i := range backups {
		backups[i].Spans = make(roachpb.Spans, spans)
		for j := range backups[i].Spans {
			backups[i].Spans[j] = makeTableSpan(uint32(100 + j + (i / 3)))
		}
		backups[i].EndTime = ts.Add(time.Minute.Nanoseconds()*int64(i), 0)
		if i > 0 {
			backups[i].StartTime = backups[i-1].EndTime
			if i%3 == 0 {
				backups[i].IntroducedSpans = roachpb.Spans{backups[i].Spans[spans-1]}
			}
		}

		files := baseFiles
		if i == 0 {
			backups[i].Files = make([]BackupManifest_File, files)
		} else {
			files = baseFiles / 2
			backups[i].Files = make([]BackupManifest_File, files)
		}

		for f := range backups[i].Files {
			start := f*5 + r.Intn(4)
			end := start + 1 + r.Intn(25)
			k := encoding.EncodeVarintAscending(backups[i].Spans[f*spans/files].Key, 1)
			k = k[:len(k):len(k)]
			backups[i].Files[f].Span.Key = encoding.EncodeVarintAscending(k, int64(start))
			backups[i].Files[f].Span.EndKey = encoding.EncodeVarintAscending(k, int64(end))
			backups[i].Files[f].Path = fmt.Sprintf("12345-b%d-f%d.sst", i, f)
		}
		// A non-nil Dir more accurately models the footprint of produced coverings.
		backups[i].Dir = roachpb.ExternalStorage{S3Config: &roachpb.ExternalStorage_S3{}}
	}
	return backups
}
