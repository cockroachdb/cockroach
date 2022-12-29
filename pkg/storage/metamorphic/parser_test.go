// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	t.Log(cfg.opts.EnsureDefaults().String())
}
