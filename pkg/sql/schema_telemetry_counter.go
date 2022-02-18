// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// IncrementDropCounterForRelation implements scbuild.TelemetryCounter.
func (p *planner) IncrementDropCounterForRelation(tbl catalog.TableDescriptor) {
	counter := sqltelemetry.SchemaChangeDropCounter(tree.GetTableType(
		tbl.IsSequence() /* isSequence */, tbl.IsView(), /* isView */
		tbl.MaterializedView()))
	telemetry.Inc(counter)
}

func (p *planner) IncrementDropCounterForSchema() {
	counter := sqltelemetry.SchemaChangeDropCounter("schema")
	sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaDrop)
	telemetry.Inc(counter)
}

func (p *planner) IncrementDropCounterForDatabase() {
	counter := sqltelemetry.SchemaChangeDropCounter("database")
	telemetry.Inc(counter)
}
