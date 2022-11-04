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

package syncexp

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var oDirect = 0

type testCase struct {
	name  string
	flags int
}

func BenchmarkFoo(b *testing.B) {
	tcs := []testCase{
		{"none", 0},                // expect this to very fast (speed of light), since we're not syncing
		{"dsync", syscall.O_DSYNC}, // expect this to be slow (why? something something flush OS write cache)
	}
	if oDirect != 0 {
		tcs = append(tcs,
			testCase{"direct", oDirect}, // expect this to be fast, since no sync
			// Expect this to be as fast as `direct` because we're ticking all the
			// boxes in [^1], so fdatasync should be free on appropriate storage.
			//
			// https://github.com/cockroachdb/cockroach/issues/88442#issuecomment-1302716873
			testCase{"dsync,direct", syscall.O_DSYNC | oDirect},
		)
	}
	for _, tc := range tcs {
		b.Run(tc.name, func(b *testing.B) {
			const writeSize = 4096 // 4kb is the typical SST block size
			numBlocks := int64(b.N)
			fileSize := numBlocks * writeSize

			path := filepath.Join(os.ExpandEnv("$HOME"), fmt.Sprintf("wal-%d.bin", fileSize))
			rand, _ := randutil.NewPseudoRand()
			if fi, err := os.Stat(path); err != nil || fi.Size() != fileSize {
				f, err := os.Create(path)
				require.NoError(b, err)
				block := make([]byte, writeSize)
				for i := int64(0); i < numBlocks; i++ {
					_, _ = rand.Read(block) // simulate WAL recycling, i.e. not zeroes
					_, err := f.WriteAt(block, i*writeSize)
					require.NoError(b, err)
				}
				require.NoError(b, f.Sync())
				require.NoError(b, f.Close())
				b.Logf("initialized %s (%s)", path, humanizeutil.IBytes(fileSize))
			}

			f, err := os.OpenFile(path, syscall.O_WRONLY|tc.flags, 0644)
			require.NoError(b, err)
			_, err = f.Seek(0, 0 /* relative to origin of file */)
			require.NoError(b, err)

			block, err := allocAlignedBuf(writeSize) // needs to have a 512-aligned addr for O_DIRECT to work
			require.NoError(b, err)
			tBegin := timeutil.Now()
			b.ResetTimer()
			for i := int64(0); i < numBlocks; i++ {
				_, _ = rand.Read(block)
				if _, err := f.WriteAt(block, i*writeSize); err != nil {
					require.NoError(b, err)
				}
			}
			require.NoError(b, f.Close())
			b.ReportMetric(float64(fileSize>>20)/timeutil.Since(tBegin).Seconds(), "mb/s")
		})
	}
}

// allocAlignedBuf allocates buffer that is aligned by blockSize.
//
// Lifted from https://github.com/brk0v/directio/blob/69406e757cf73049ae21874b8d101287ecddbeb7/directio.go#L18-L53.
func allocAlignedBuf(n int) ([]byte, error) {
	if n == 0 {
		return nil, errors.New("size is `0` can't allocate buffer")
	}

	const blockSize = 512

	// align returns an offset for alignment.
	align := func(b []byte) int {
		return int(uintptr(unsafe.Pointer(&b[0])) & uintptr(blockSize-1))
	}

	// Allocate memory buffer
	buf := make([]byte, n+blockSize)

	// First memmory alignment
	a1 := align(buf)
	offset := 0
	if a1 != 0 {
		offset = blockSize - a1
	}

	buf = buf[offset : offset+n]

	// Was alredy aligned. So just exit
	if a1 == 0 {
		return buf, nil
	}

	// Second alignment – check and exit
	a2 := align(buf)
	if a2 != 0 {
		return nil, errors.New("can't allocate aligned buffer")
	}

	return buf, nil
}
