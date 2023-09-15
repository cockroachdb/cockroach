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

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// eventMemoryMultipier is the multiplier for the amount of memory needed to
// export a datum. Datums may be encoded into bytes and is buffered until the
// export file is written out. Since it's difficult to calculate the number of
// bytes that will be created, we use this multiplier for estimation.
var eventMemoryMultipier = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"export.event_memory_multiplier",
	"the amount of memory required to export a datum is multiplied by this factor",
	3,
	func(v float64) error {
		if v < 1 {
			return errors.New("value must be at least 1")
		}
		return nil
	},
)

// ExportTestingKnobs contains testing knobs for Export.
type ExportTestingKnobs struct {
	// MemoryMonitor is a test memory monitor to report allocations to.
	MemoryMonitor *mon.BytesMonitor
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExportTestingKnobs) ModuleTestingKnobs() {}
