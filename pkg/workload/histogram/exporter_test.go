// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package histogram

import (
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHdrJsonExporter_Validate(t *testing.T) {
	exporter := &HdrJsonExporter{}

	err := exporter.Validate("metrics.json")
	assert.NoError(t, err)

	err = exporter.Validate("metrics.txt")
	assert.Error(t, err)
	assert.Equal(t, "file path must end with .json", err.Error())
}

func TestHdrJsonExporter_Init(t *testing.T) {
	exporter := &HdrJsonExporter{}
	file, err := os.CreateTemp("", "metrics.json")
	writer := io.Writer(file)
	assert.NoError(t, err)
	defer removeFile(file.Name())

	err = exporter.Init(&writer)
	assert.NoError(t, err)
	assert.NotNil(t, exporter.jsonEnc)
}

func TestHdrJsonExporter_SnapshotAndWrite(t *testing.T) {
	exporter := &HdrJsonExporter{}
	file, err := os.CreateTemp("", "metrics.json")
	writer := io.Writer(file)
	assert.NoError(t, err)
	defer removeFile(file.Name())

	reg := NewRegistryWithExporter(
		time.Second,
		MockWorkloadName,
		exporter,
	)

	err = exporter.Init(&writer)
	assert.NoError(t, err)

	start := time.Time{}

	reg.GetHandle().Get("test").Record(time.Second / 2)
	reg.Tick(func(tick Tick) {
		// Make output deterministic.
		tick.Elapsed = time.Second / 2
		tick.Now = start.Add(tick.Elapsed)

		err = tick.Exporter.SnapshotAndWrite(tick)
		require.NoError(t, err)
		err = exporter.Close(nil)
		require.NoError(t, err)
	})

	var data map[string]interface{}
	buf, err := os.ReadFile(file.Name())
	require.NoError(t, err)
	err = json.Unmarshal(buf, &data)
	assert.NoError(t, err)
	assert.Equal(t, "test", data["Name"])
}

func TestHdrJsonExporter_Close(t *testing.T) {
	exporter := &HdrJsonExporter{}
	file, err := os.CreateTemp("", "metrics.json")
	writer := io.Writer(file)
	assert.NoError(t, err)
	defer removeFile(file.Name())

	err = exporter.Init(&writer)
	assert.NoError(t, err)

	err = exporter.Close(nil)
	assert.NoError(t, err)
}

func TestOpenmetricsExporter_Validate(t *testing.T) {
	exporter := &OpenmetricsExporter{}

	err := exporter.Validate("metrics.txt")
	assert.NoError(t, err)

	err = exporter.Validate("metrics.json")
	assert.Error(t, err)
	assert.Equal(t, "file path must not end with .json", err.Error())
}

func TestOpenmetricsExporter_Init(t *testing.T) {
	exporter := &OpenmetricsExporter{}
	file, err := os.CreateTemp("", "metrics.txt")
	writer := io.Writer(file)
	assert.NoError(t, err)
	defer removeFile(file.Name())

	err = exporter.Init(&writer)
	assert.NoError(t, err)
	assert.NotNil(t, exporter.writer)
}

func TestOpenmetricsExporter_SnapshotAndWrite(t *testing.T) {
	exporter := &OpenmetricsExporter{}
	file, err := os.CreateTemp("", "metrics.txt")
	writer := io.Writer(file)
	assert.NoError(t, err)
	defer removeFile(file.Name())

	reg := NewRegistryWithExporter(
		time.Second,
		MockWorkloadName,
		exporter,
	)

	err = exporter.Init(&writer)
	assert.NoError(t, err)

	start := time.Time{}

	reg.GetHandle().Get("read").Record(time.Second / 2)
	reg.Tick(func(tick Tick) {
		// Make output deterministic.
		tick.Elapsed = time.Second / 2
		tick.Now = start.Add(tick.Elapsed)

		err = tick.Exporter.SnapshotAndWrite(tick)
		require.NoError(t, err)
		err = exporter.Close(nil)
		require.NoError(t, err)
	})

	buf, _ := os.ReadFile(file.Name())
	assert.Contains(t, string(buf), "# TYPE")
}

func TestOpenmetricsExporter_Close(t *testing.T) {
	exporter := &OpenmetricsExporter{}
	file, err := os.CreateTemp("", "metrics.txt")
	writer := io.Writer(file)
	assert.NoError(t, err)
	defer removeFile(file.Name())

	err = exporter.Init(&writer)
	assert.NoError(t, err)

	err = exporter.Close(nil)
	assert.NoError(t, err)
}

func removeFile(file string) {
	_ = os.Remove(file)
}
