// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exporter

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/codahale/hdrhistogram"
)

// HdrJsonExporter exports hdr json metrics specified in SnapshotTick
type HdrJsonExporter struct {
	jsonEnc *json.Encoder
}

func (h *HdrJsonExporter) Validate(filePath string) error {
	if !strings.HasSuffix(filePath, ".json") {
		return fmt.Errorf("file path must end with .json")
	}
	return nil
}

func (h *HdrJsonExporter) Init(w *io.Writer) {
	h.jsonEnc = json.NewEncoder(*w)
}

func (h *HdrJsonExporter) SnapshotAndWrite(
	hist *hdrhistogram.Histogram, now time.Time, elapsed time.Duration, name *string,
) error {
	if err := h.jsonEnc.Encode(&SnapshotTick{
		Name:    *name,
		Hist:    hist.Export(),
		Elapsed: elapsed,
		Now:     now,
	}); err != nil {
		return err
	}
	return nil
}

func (h *HdrJsonExporter) Close(f func() error) error {
	if f != nil {
		return f()
	}
	return nil
}

// SnapshotTick parallels Tick but replace the histogram with a
// snapshot that is suitable for serialization. Additionally, it only contains
// the per-tick histogram, not the cumulative histogram. (The cumulative
// histogram can be computed by aggregating all of the per-tick histograms).
type SnapshotTick struct {
	Name    string
	Hist    *hdrhistogram.Snapshot
	Elapsed time.Duration
	Now     time.Time
}
