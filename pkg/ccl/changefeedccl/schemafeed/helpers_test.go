// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package schemafeed

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

const TestingAllEventFilter = "testing"

func init() {
	schemaChangeEventFilters[TestingAllEventFilter] = tableEventFilter{
		tableEventDropColumn:                  false,
		tableEventAddColumnWithBackfill:       false,
		tableEventAddColumnNoBackfill:         false,
		tableEventUnknown:                     false,
		tableEventPrimaryKeyChange:            false,
		tableEventLocalityRegionalByRowChange: false,
		tableEventAddHiddenColumn:             false,
	}
}

var ClassifyEvent = classifyTableEvent

func PrintTableEventType(t tableEventType) string {
	var strs []string
	for i := 0; i < 63; i++ {
		if t&1<<i != 0 {
			strs = append(strs, tableEventType(1<<i).String())
		}
	}
	return strings.Join(strs, "|")
}

func CreateChangefeedTargets(tableID descpb.ID) changefeedbase.Targets {
	targets := changefeedbase.Targets{}
	targets.Add(changefeedbase.Target{TableID: tableID})
	return targets
}
