// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

const getVirtualSchemaEntry = "sql.schema.get_virtual_table.%s.%s"

// trackedSchemas have the schemas that we track by telemetry.
var trackedSchemas = map[string]struct{}{
	"pg_catalog":         {},
	"information_schema": {},
}

// IncrementGetVirtualTableEntry is used to increment telemetry counter for any
// use of tracked schemas tables.
func IncrementGetVirtualTableEntry(schema, tableName string) {
	if _, ok := trackedSchemas[schema]; ok {
		telemetry.Inc(telemetry.GetCounter(fmt.Sprintf(getVirtualSchemaEntry, schema, tableName)))
	}
}
