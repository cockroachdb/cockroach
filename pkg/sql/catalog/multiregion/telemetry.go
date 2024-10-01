// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Constants to use for telemetry for multi-region table localities.
const (
	TelemetryNameGlobal            = "global"
	TelemetryNameRegionalByTable   = "regional_by_table"
	TelemetryNameRegionalByTableIn = "regional_by_table_in"
	TelemetryNameRegionalByRow     = "regional_by_row"
	TelemetryNameRegionalByRowAs   = "regional_by_row_as"
)

// TelemetryNameForLocality returns the telemetry name for a given locality level.
func TelemetryNameForLocality(node *tree.Locality) string {
	switch node.LocalityLevel {
	case tree.LocalityLevelGlobal:
		return TelemetryNameGlobal
	case tree.LocalityLevelTable:
		if node.TableRegion != tree.RegionalByRowRegionNotSpecifiedName {
			return TelemetryNameRegionalByTableIn
		}
		return TelemetryNameRegionalByTable
	case tree.LocalityLevelRow:
		if node.RegionalByRowColumn != tree.PrimaryRegionNotSpecifiedName {
			return TelemetryNameRegionalByRowAs
		}
		return TelemetryNameRegionalByRow
	default:
		panic(fmt.Sprintf("unknown locality: %#v", node.LocalityLevel))
	}
}

// TelemetryNameForLocalityConfig returns the name to use for the given
// locality config.
func TelemetryNameForLocalityConfig(cfg *catpb.LocalityConfig) (string, error) {
	switch l := cfg.Locality.(type) {
	case *catpb.LocalityConfig_Global_:
		return TelemetryNameGlobal, nil
	case *catpb.LocalityConfig_RegionalByTable_:
		if l.RegionalByTable.Region != nil {
			return TelemetryNameRegionalByTableIn, nil
		}
		return TelemetryNameRegionalByTable, nil
	case *catpb.LocalityConfig_RegionalByRow_:
		if l.RegionalByRow.As != nil {
			return TelemetryNameRegionalByRowAs, nil
		}
		return TelemetryNameRegionalByRow, nil
	}
	return "", errors.AssertionFailedf(
		"unknown locality config TelemetryNameForLocality: type %T",
		cfg.Locality,
	)
}
