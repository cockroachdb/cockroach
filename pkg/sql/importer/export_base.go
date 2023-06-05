// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

// ExportTestingKnobs contains testing knobs for Export.
type ExportTestingKnobs struct {
	// EnableParquetTestMetadata makes `EXPORT INTO ` with the
	// parquet format write CRDB-specific metadata that is required
	// for tests to read raw data in parquet files into CRDB datums.
	EnableParquetTestMetadata bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExportTestingKnobs) ModuleTestingKnobs() {}
