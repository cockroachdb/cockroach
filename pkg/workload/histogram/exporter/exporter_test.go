// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exporter

import (
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	mockHistogramName = "mock_histogram"
)

func TestHdrJsonExporter(t *testing.T) {
	exporter := &HdrJsonExporter{}
	t.Run("Validate", func(t *testing.T) {
		err := exporter.Validate("metrics.json")
		assert.NoError(t, err)

		err = exporter.Validate("metrics.txt")
		assert.Error(t, err)
		assert.Equal(t, "file path must end with .json", err.Error())
	})

	t.Run("Init", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.json")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)
	})

	t.Run("SnapshotAndWrite", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.json")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		start := time.Time{}
		elapsed := time.Second / 2
		name := mockHistogramName
		mockHistogram := hdrhistogram.New(0, 100, 1)
		err = exporter.SnapshotAndWrite(mockHistogram, start, elapsed, &name)
		require.NoError(t, err)
		err = exporter.Close(nil)
		require.NoError(t, err)

		var data map[string]interface{}
		buf, err := os.ReadFile(file.Name())
		require.NoError(t, err)
		err = json.Unmarshal(buf, &data)
		require.NoError(t, err)
		assert.Equal(t, "mock_histogram", data["Name"])

	})

	t.Run("Close", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.json")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		err = exporter.Close(nil)
		assert.NoError(t, err)
	})
}

func TestOpenmetricsExporter(t *testing.T) {
	exporter := &OpenmetricsExporter{}
	t.Run("Validate", func(t *testing.T) {
		err := exporter.Validate("metrics.txt")
		assert.NoError(t, err)

		err = exporter.Validate("metrics.json")
		assert.Error(t, err)
		assert.Equal(t, "file path must not end with .json", err.Error())
	})

	t.Run("Init", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.txt")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)
		assert.NotNil(t, exporter.writer)
	})

	t.Run("SnapshotAndWrite", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.txt")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		start := time.Time{}
		elapsed := time.Second / 2
		name := mockHistogramName
		mockHistogram := hdrhistogram.New(0, 100, 1)
		err = exporter.SnapshotAndWrite(mockHistogram, start, elapsed, &name)
		require.NoError(t, err)
		err = exporter.Close(nil)
		require.NoError(t, err)

		buf, _ := os.ReadFile(file.Name())
		assert.Contains(t, string(buf), "# TYPE")
	})

	t.Run("Close", func(t *testing.T) {
		file, err := os.CreateTemp("", "metrics.txt")
		require.NoError(t, err)
		defer removeFile(file.Name(), t)

		writer := io.Writer(file)
		exporter.Init(&writer)
		require.NoError(t, err)

		err = exporter.Close(nil)
		assert.NoError(t, err)
	})
}

func removeFile(file string, t *testing.T) {
	assert.NoError(t, os.Remove(file))
}
