// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlcommenter

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

func TestExtractTagsDataDriven(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "extract":
				comment := d.Input

				tags := ExtractQueryTags(comment)
				jsonResult, err := json.MarshalIndent(tags, "", "  ")
				if err != nil {
					return fmt.Sprintf("Error marshaling result to JSON: %v", err)
				}

				return string(jsonResult)
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}
