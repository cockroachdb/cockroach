// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// eventMemoryMultipier is the multiplier for the amount of memory needed to
// export a datum. Datums may be encoded into bytes and is buffered until the
// export file is written out. Since it's difficult to calculate the number of
// bytes that will be created, we use this multiplier for estimation.
var eventMemoryMultipier = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"export.event_memory_multiplier",
	"the amount of memory required to export a datum is multiplied by this factor",
	3,
	settings.FloatWithMinimum(1),
)

// ExportTestingKnobs contains testing knobs for Export.
type ExportTestingKnobs struct {
	// MemoryMonitor is a test memory monitor to report allocations to.
	MemoryMonitor *mon.BytesMonitor
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExportTestingKnobs) ModuleTestingKnobs() {}
