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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql"
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
		b.Run(fmt.Sprintf("numBackups=%d", numBackups), func(b *testing.B) {
			for _, numSpans := range []int{10, 20, 100} {
				b.Run(fmt.Sprintf("numSpans=%d", numSpans), func(b *testing.B) {
					for _, baseFiles := range []int{0, 10, 100, 1000, 10000} {
						b.Run(fmt.Sprintf("numFiles=%d", baseFiles), func(b *testing.B) {
							for _, slimManifest := range []bool{true, false} {
								b.Run(fmt.Sprintf("slim=%t", slimManifest), func(b *testing.B) {
									backups, err := MockBackupChain(ctx, numBackups, numSpans, baseFiles, r, slimManifest, execCfg)
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
		b.Run(fmt.Sprintf("numBackups=%d", numBackups), func(b *testing.B) {
			for _, baseFiles := range []int{0, 100, 10000} {
				b.Run(fmt.Sprintf("numFiles=%d", baseFiles), func(b *testing.B) {
					for _, numSpans := range []int{10, 100} {
						b.Run(fmt.Sprintf("numSpans=%d", numSpans), func(b *testing.B) {
							for _, slimManifest := range []bool{true, false} {
								b.Run(fmt.Sprintf("slimManifest=%t", slimManifest), func(b *testing.B) {
									backups, err := MockBackupChain(ctx, numBackups, numSpans, baseFiles, r, slimManifest, execCfg)
									require.NoError(b, err)
									b.ResetTimer()
									for i := 0; i < b.N; i++ {
										if err := checkCoverage(ctx, backups[numBackups-1].Spans, backups); err != nil {
											b.Fatal(err)
										}
										introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, hlc.Timestamp{})
										require.NoError(b, err)

										layerToBackupManifestFileIterFactory, err := getBackupManifestFileIters(ctx, &execCfg,
											backups, nil, nil)
										require.NoError(b, err)
										cov, err := makeSimpleImportSpans(backups[numBackups-1].Spans, backups,
											layerToBackupManifestFileIterFactory, nil, introducedSpanFrontier,
											nil, 0)
										require.NoError(b, err)
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
