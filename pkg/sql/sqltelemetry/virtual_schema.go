// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
