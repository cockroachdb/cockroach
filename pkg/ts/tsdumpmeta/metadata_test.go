// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsdumpmeta

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestMetadataReadWrite tests the core functionality for writing and reading
// embedded metadata in tsdump files, including error handling for invalid data.
func TestMetadataReadWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("Write Success", func(t *testing.T) {
		var buf bytes.Buffer
		metadata := Metadata{
			Version: "v23.1.0",
			StoreToNodeMap: map[string]string{
				"1": "1",
				"2": "1",
				"3": "2",
				"4": "2",
			},
			CreatedAt: timeutil.Unix(1609459200, 0), // 2021-01-01 00:00:00 UTC
		}

		err := Write(&buf, metadata)
		require.NoError(t, err)
		require.Greater(t, buf.Len(), 0, "should have written some data")
	})

	t.Run("Read Success", func(t *testing.T) {
		originalMetadata := Metadata{
			Version: "v23.2.1",
			StoreToNodeMap: map[string]string{
				"1": "1",
				"2": "2",
				"3": "3",
			},
			CreatedAt: timeutil.Unix(1640995200, 0), // 2022-01-01 00:00:00 UTC
		}

		var buf bytes.Buffer
		err := Write(&buf, originalMetadata)
		require.NoError(t, err)

		dec := gob.NewDecoder(&buf)
		readMetadata, err := Read(dec)
		require.NoError(t, err)
		require.NotNil(t, readMetadata)

		require.Equal(t, originalMetadata.Version, readMetadata.Version)
		require.Equal(t, originalMetadata.StoreToNodeMap, readMetadata.StoreToNodeMap)
		require.Equal(t, originalMetadata.CreatedAt.Unix(), readMetadata.CreatedAt.Unix())
	})

	t.Run("Read No Metadata", func(t *testing.T) {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		kv := roachpb.KeyValue{
			Key:   roachpb.Key("test-key"),
			Value: roachpb.MakeValueFromString("test-value"),
		}
		err := enc.Encode(kv)
		require.NoError(t, err)

		dec := gob.NewDecoder(&buf)
		_, err = Read(dec)
		require.Error(t, err)
	})

	t.Run("Read Invalid Metadata", func(t *testing.T) {
		// Create a buffer with corrupted/non-gob data so decoding fails
		var buf bytes.Buffer
		_, _ = buf.Write([]byte("this-is-not-valid-gob-metadata"))

		dec := gob.NewDecoder(&buf)
		_, err := Read(dec)
		require.Error(t, err)
	})
}
