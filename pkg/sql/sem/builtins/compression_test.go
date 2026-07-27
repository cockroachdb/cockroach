// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDecompressLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Exercise the size cap with a small limit rather than the production
	// builtinconstants.MaxAllocatedStringSize (128 MiB). The enforcement logic
	// is identical at any limit, and using the real limit made the test
	// allocate hundreds of MiB, which OOM-killed the test process on CI.
	const maxSize = 1 << 20 // 1 MiB
	bomb := bytes.Repeat([]byte("a"), maxSize+1)
	fits := bytes.Repeat([]byte("a"), maxSize)

	for _, codec := range []string{"gzip", "zstd", "lz4", "snappy"} {
		t.Run(codec+"/bomb rejected", func(t *testing.T) {
			compressed, err := compress(bomb, codec)
			require.NoError(t, err)
			require.Less(t, len(compressed), maxSize,
				"input must actually compress to be a bomb")

			_, err = decompressWithLimit(compressed, codec, maxSize)
			require.True(t, errors.Is(err, errStringTooLarge),
				"expected errStringTooLarge, got: %v", err)
			require.Equal(t, pgcode.ProgramLimitExceeded, pgerror.GetPGCode(err))
		})

		t.Run(codec+"/cap-sized payload round-trips", func(t *testing.T) {
			compressed, err := compress(fits, codec)
			require.NoError(t, err)
			decompressed, err := decompressWithLimit(compressed, codec, maxSize)
			require.NoError(t, err)
			require.Equal(t, len(fits), len(decompressed))
		})
	}
}
