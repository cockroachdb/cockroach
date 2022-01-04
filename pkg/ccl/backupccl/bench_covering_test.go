// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	fmt "fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func BenchmarkCoverageChecks(b *testing.B) {
	for _, numBackups := range []int{1, 7, 24, 24 * 4} {
		b.Run(fmt.Sprintf("numBackups=%d", numBackups), func(b *testing.B) {
			for _, numSpans := range []int{10, 20, 100} {
				b.Run(fmt.Sprintf("numSpans=%d", numSpans), func(b *testing.B) {
					for _, numFiles := range []int{0, 10, 100, 1000, 10000} {
						b.Run(fmt.Sprintf("numFiles=%d", numFiles), func(b *testing.B) {
							b.StopTimer()
							backups := make([]BackupManifest, numBackups)
							ts := hlc.Timestamp{WallTime: time.Second.Nanoseconds()}
							for i := range backups {
								backups[i].Spans = make(roachpb.Spans, numSpans)
								for j := range backups[i].Spans {
									backups[i].Spans[j] = makeTableSpan(uint32(100 + j + (i / 4)))
								}
								backups[i].EndTime = ts.Add(time.Minute.Nanoseconds()*int64(i), 0)
								if i > 0 {
									backups[i].StartTime = backups[i-1].EndTime
									if i%4 == 0 {
										backups[i].IntroducedSpans = roachpb.Spans{backups[i].Spans[numSpans-1]}
									}
								}

								if i == 0 {
									backups[i].Files = make([]BackupManifest_File, numFiles)
								} else {
									numFiles = numFiles / 2
									backups[i].Files = make([]BackupManifest_File, numFiles)
								}

								for f := range backups[i].Files {
									backups[i].Files[f].Path = fmt.Sprintf("1234567890%d.sst", f)
									backups[i].Files[f].Span.Key = encoding.EncodeVarintAscending(backups[i].Spans[f*numSpans/numFiles].Key, int64(f))
									backups[i].Files[f].Span.EndKey = encoding.EncodeVarintAscending(backups[i].Spans[f*numSpans/numFiles].Key, int64(f+1))

								}
								backups[i].Dir = roachpb.ExternalStorage{S3Config: &roachpb.ExternalStorage_S3{
									Bucket:    "some-string-name",
									Prefix:    "some-string-path/to/some/file",
									AccessKey: "some-access-key",
									Secret:    "some-secret-key",
								}}
							}
							b.ResetTimer()

							ctx := context.Background()
							b.Run("checkCoverage", func(b *testing.B) {
								b.ResetTimer()
								for i := 0; i < b.N; i++ {
									if err := checkCoverage(ctx, backups[numBackups-1].Spans, backups); err != nil {
										b.Fatal(err)
									}
								}
							})
							b.Run("makeImportSpans", func(b *testing.B) {
								b.ResetTimer()
								for i := 0; i < b.N; i++ {
									_, ts, err := makeImportSpans(backups[numBackups-1].Spans, backups, nil, nil, errOnMissingRange)
									if err != nil {
										b.Fatal(err)
									}
									if got, expected := ts, backups[len(backups)-1].EndTime; !got.Equal(expected) {
										b.Fatal(expected, got)
									}
								}
							})
						})
					}
				})
			}
		})
	}
}
