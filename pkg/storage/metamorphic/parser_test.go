// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metamorphic

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestParseOutputPreamble(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := os.Open(datapathutils.TestDataPath(t, "sample.meta"))
	require.NoError(t, err)

	cfg, seed, err := parseOutputPreamble(f)
	require.NoError(t, err)
	require.Equal(t, seed, int64(7375396416917217630))
	require.Equal(t, cfg.name, "random-007")
	// TODO(jackson): Assert roundtrip equality.
	cfg.opts.EnsureDefaults()
	t.Log(cfg.opts.String())
}
