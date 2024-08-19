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
	"io"
	"time"

	"github.com/codahale/hdrhistogram"
)

// Exporter is used to export workload histogram metrics to a file that is pre-created.
type Exporter interface {
	// Validate is an optional method that can be used to do any kind of validation of the filepath provided.
	// Eg: in a json exporter, the file name extension should be json etc.
	// This should be called before Init
	Validate(filePath string) error
	// Init is used to initialize objects of the exporter. Should be called after Validation.
	Init(w *io.Writer) error
	// SnapshotAndWrite is used to take the snapshot of the histogram and write to the os.File provided in Init
	SnapshotAndWrite(hist *hdrhistogram.Histogram, now time.Time, elapsed time.Duration, name *string) error
	// Close is used to close and clean any objects that were initialized in Init. Should be called at the end of your program.
	Close(f func() error) error
}
