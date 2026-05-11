// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/text"
	"github.com/stretchr/testify/require"
)

// TestWriteInitialClusterData bootstraps multiple ranges and captures the WAG
// nodes written to the log engine.
func TestWriteInitialClusterData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Disable metamorphic values for deterministic output.
	storage.DisableMetamorphicSimpleValueEncoding(t)

	run := func(t *testing.T, goldenFile string, eng storage.Engine, engines kvstorage.Engines) {
		// Use a fixed version and timestamp so the golden file is deterministic.
		version := roachpb.Version{Major: 10, Minor: 2, Patch: 17}
		const nowNanos = 1e9 // 1 second

		// Include Meta2Prefix to match real bootstrap — without it, every range
		// writes to the same meta1 key and triggers WriteTooOldError.
		splits := []roachpb.RKey{
			roachpb.RKey(keys.Meta2Prefix), roachpb.RKey("m"), roachpb.RKey("z"),
		}
		require.NoError(t, WriteInitialClusterData(
			context.Background(), engines, nil, /* initialValues */
			version, 1 /* numStores */, splits, nowNanos,
			StoreTestingKnobs{},
		))

		// Iterate WAG nodes from the log engine and format them.
		ctx := context.Background()
		var sb strings.Builder
		var it wag.Iterator
		for index, node := range it.Iter(ctx, eng) {
			fmt.Fprintf(&sb, "wag/%d: ", index)
			for i, e := range node.Events {
				if i > 0 {
					sb.WriteByte(' ')
				}
				fmt.Fprintf(&sb, "%s", e)
			}
			sb.WriteByte('\n')
			if b := node.Mutation.Batch; len(b) > 0 {
				decoded, err := print.DecodeWriteBatch(b)
				require.NoError(t, err)
				sb.WriteString(text.Indent(decoded, "> "))
			}
		}
		require.NoError(t, it.Error())

		output := strings.ReplaceAll(sb.String(), "\n\n", "\n")
		echotest.Require(t, output, filepath.Join(
			"testdata", "TestWriteInitialClusterData", goldenFile,
		))
	}

	t.Run("shared-eng", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()
		run(t, "shared-eng.txt", eng, kvstorage.MakeEngines(eng))
	})

	t.Run("sep-eng", func(t *testing.T) {
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()
		run(t, "sep-eng.txt", eng, kvstorage.MakeSeparatedEnginesForTesting(eng, eng))
	})
}
