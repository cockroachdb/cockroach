// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func BenchmarkCoverageChecks(b *testing.B) {
	tc, _, _, cleanupFn := backupRestoreTestSetup(b, singleNode, 1, InitManualReplication)
	defer cleanupFn()
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	ctx := context.Background()
	r, _ := randutil.NewTestRand()

	for _, numBackups := range []int{1, 7, 24, 24 * 4} {
		numBackups := numBackups
		b.Run(fmt.Sprintf("numBackups=%d", numBackups), func(b *testing.B) {
			for _, numSpans := range []int{10, 20, 100} {
				b.Run(fmt.Sprintf("numSpans=%d", numSpans), func(b *testing.B) {
					for _, baseFiles := range []int{0, 10, 100, 1000, 10000} {
						b.Run(fmt.Sprintf("numFiles=%d", baseFiles), func(b *testing.B) {
							for _, hasExternalFilesList := range []bool{true, false} {
								b.Run(fmt.Sprintf("slim=%t", hasExternalFilesList), func(b *testing.B) {
									backups, err := MockBackupChain(ctx, numBackups, numSpans, baseFiles, 1<<20, r, hasExternalFilesList, execCfg)
									require.NoError(b, err)
									b.ResetTimer()

									for i := 0; i < b.N; i++ {
										if err := checkCoverage(ctx, backups[numBackups-1].Spans, backups); err != nil {
											b.Fatal(err)
										}
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkRestoreEntryCover(b *testing.B) {
	tc, _, _, cleanupFn := backupRestoreTestSetup(b, singleNode, 1, InitManualReplication)
	defer cleanupFn()
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)

	ctx := context.Background()
	r, _ := randutil.NewTestRand()
	for _, numBackups := range []int{1, 2, 24, 24 * 4} {
		numBackups := numBackups
		b.Run(fmt.Sprintf("numBackups=%d", numBackups), func(b *testing.B) {
			for _, baseFiles := range []int{0, 100, 10000} {
				b.Run(fmt.Sprintf("numFiles=%d", baseFiles), func(b *testing.B) {
					for _, numSpans := range []int{10, 100} {
						b.Run(fmt.Sprintf("numSpans=%d", numSpans), func(b *testing.B) {
							for _, hasExternalFilesList := range []bool{true, false} {
								b.Run(fmt.Sprintf("hasExternalFilesList=%t", hasExternalFilesList),
									func(b *testing.B) {
										backups, err := MockBackupChain(ctx, numBackups, numSpans, baseFiles, 1<<20, r, hasExternalFilesList, execCfg)
										require.NoError(b, err)
										b.ResetTimer()
										for i := 0; i < b.N; i++ {
											if err := checkCoverage(ctx, backups[numBackups-1].Spans, backups); err != nil {
												b.Fatal(err)
											}
											introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, hlc.Timestamp{})
											require.NoError(b, err)

											layerToBackupManifestFileIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, execCfg.DistSQLSrv.ExternalStorage,
												backups, nil, nil)
											require.NoError(b, err)

											spanCh := make(chan execinfrapb.RestoreSpanEntry, 1000)

											filter, err := makeSpanCoveringFilter(
												backups[numBackups-1].Spans,
												[]jobspb.RestoreProgress_FrontierEntry{},
												introducedSpanFrontier,
												0,
												defaultMaxFileCount,
											)
											require.NoError(b, err)
											defer filter.close()

											g := ctxgroup.WithContext(ctx)
											g.GoCtx(func(ctx context.Context) error {
												defer close(spanCh)
												return generateAndSendImportSpans(
													ctx,
													backups[numBackups-1].Spans,
													backups,
													layerToBackupManifestFileIterFactory,
													nil,
													filter,
													&inclusiveEndKeyComparator{},
													spanCh)
											})

											var cov []execinfrapb.RestoreSpanEntry
											for entry := range spanCh {
												cov = append(cov, entry)
											}

											require.NoError(b, g.Wait())
											b.ReportMetric(float64(len(cov)), "coverSize")
										}
									})
							}
						})
					}
				})
			}
		})
	}
}
