// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/gcp"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/stretchr/testify/require"
)

var (
	gcsPrefix = `gs://cockroach-fixtures-us-east1/benchdata/copyssts/?AUTH=implicit`
	s3Prefix  = `s3://cockroach-fixtures-us-east-2/benchdata/copyssts/?AUTH=implicit`
	// files are ssts selected -- at random -- from a backup of tpcc 150k backed
	// up by 24.1. While any one file would have been enough to write a benchmark,
	// an unlucky random choice could have a region unusually compression-friendly
	// or unfriendly, thus we have a sampling of files and for higher b.N counts
	// we can cycle them. All selected files are >100mib, to ensure a 64mb read up
	// to 32mb offset can be satisfied.
	files = []string{
		`969308161621262343.sst`, // 135.73 MiB
		`969324898841395202.sst`, // 144.84 MiB
		`969328101645746185.sst`, // 109.09 MiB
		`969307702044262410.sst`, // 135.56 MiB
		`969327387185250307.sst`, // 109.42 MiB
		`969317317823365130.sst`, // 145.84 MiB
		`969330433561821186.sst`, // 108.79 MiB
		`969312935730315269.sst`, // 136.98 MiB
		`969316100247453706.sst`, // 112.23 MiB
	}
)

func BenchmarkObjStorageCopyGCS(b *testing.B) {
	benchObjstorageCopy(b, gcsPrefix, files)
}

func BenchmarkObjStorageCopyS3(b *testing.B) {
	benchObjstorageCopy(b, s3Prefix, files)
}

var runCloudBenches = envutil.EnvOrDefaultBool("COCKROACH_BENCHMARK_REMOTE_SSTS", false)

func benchObjstorageCopy(b *testing.B, prefix string, suffixes []string) {
	if !runCloudBenches {
		skip.IgnoreLint(b, "only run manually with BENCHMARK_REMOTE_SSTS to bench cloud io")
	}

	ctx := context.Background()
	b.StopTimer()
	st := cluster.MakeTestingClusterSettings()
	cfg := base.ExternalIODirConfig{}
	d, err := cloud.ExternalStorageConfFromURI(prefix, username.SQLUsername{})
	require.NoError(b, err)

	es, err := cloud.MakeEarlyBootExternalStorage(ctx, d, cfg, st, nil, cloud.NilMetrics)
	require.NoError(b, err)
	defer es.Close()

	s := MakeExternalStorageWrapper(ctx, es)
	b.ResetTimer()
	b.StartTimer()

	for _, size := range []int64{4 << 10, 64 << 10, 1 << 20, 8 << 20, 32 << 20, 64 << 20} {
		b.Run(fmt.Sprintf("size=%s", humanizeutil.IBytes(size)), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				suffix := suffixes[i%len(suffixes)]
				r, fileSize, err := s.ReadObject(ctx, suffix)
				if err != nil {
					b.Fatal(err)
				}

				readable := objstorageprovider.NewRemoteReadable(r, fileSize)
				rh := readable.NewReadHandle(0 /* readBeforeSize */)
				if err := objstorage.Copy(ctx, rh, discard{}, 4<<20, uint64(size)); err != nil {
					b.Fatal(err)
				}
				if err := rh.Close(); err != nil {
					b.Fatal(err)
				}
				if err := readable.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.SetBytes(size)
		})
	}
}

type discard struct{}

var _ objstorage.Writable = discard{}

func (discard) Write(p []byte) error { return nil }
func (discard) Finish() error        { return nil }
func (discard) Abort()               {}
