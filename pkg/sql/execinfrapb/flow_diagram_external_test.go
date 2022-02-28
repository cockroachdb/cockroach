// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPlanDiagramTableReaderWrapColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc, err := sql.CreateTestTableDescriptor(
		context.Background(),
		1, 100,
		`CREATE TABLE test (
		  this_is_a_very_long_name_01 INT PRIMARY KEY,
		  this_is_a_very_long_name_02 INT,
		  this_is_a_very_long_name_03 INT,
		  this_is_a_very_long_name_04 INT,
		  this_is_a_very_long_name_05 INT,
		  this_is_a_very_long_name_06 INT,
		  this_is_a_very_long_name_07 INT,
		  this_is_a_very_long_name_08 INT,
		  this_is_a_very_long_name_09 INT,
		  this_is_a_very_long_name_10 INT,
		  this_is_a_very_long_name_11 INT,
		  this_is_a_very_long_name_12 INT,
		  this_is_a_very_long_name_13 INT,
		  this_is_a_very_long_name_14 INT,
		  this_is_a_very_long_name_15 INT,
		  this_is_a_very_long_name_16 INT,
		  x INT,
		  y INT,
		  this_is_a_super_duper_long_name_that_is_longer_than_any_reasonable_wrapping_limit_and_must_appear_on_its_own_line INT,
		  z INT
		)`,
		catpb.NewBasePrivilegeDescriptor(security.NodeUserName()),
	)
	if err != nil {
		t.Fatal(err)
	}

	flows := make(map[base.SQLInstanceID]*execinfrapb.FlowSpec)

	var tr execinfrapb.TableReaderSpec
	if err := rowenc.InitIndexFetchSpec(
		&tr.FetchSpec, keys.SystemSQLCodec, desc, desc.GetPrimaryIndex(),
		[]descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	); err != nil {
		t.Fatal(err)
	}

	flows[1] = &execinfrapb.FlowSpec{
		Processors: []execinfrapb.ProcessorSpec{{
			Core: execinfrapb.ProcessorCoreUnion{TableReader: &tr},
			Output: []execinfrapb.OutputRouterSpec{{
				Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
				Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointSpec_SYNC_RESPONSE}},
			}},
			StageID:     1,
			ProcessorID: 0,
		}},
	}

	_, url, err := execinfrapb.GeneratePlanDiagramURL("SOME SQL HERE", flows, execinfrapb.DiagramFlags{})
	if err != nil {
		t.Fatal(err)
	}
	expectedURL := "https://cockroachdb.github.io/distsqlplan/decode.html#eJyMk1Hr0zAUxd_9FOU-B2w2N7VPggwcqNPNNymX2Fy6YJrE3NStjn53afswFP7Z_yVwz--eEwgnN-BfFio4HT7titPXj8WH3XEHApzX9Fl1xFB9Bwm1gBB9Q8w-TtJtXtjrK1SlAONCnya5FtD4SFDdIJlkCSr4pn5YOpLSFF-WIEBTUsbOsYk4vZsODD9pAAHvve07x1WRzobRMCr8TXFA612LTnWEpRQZuMrBtQABGf4qZ97k4PZB8uuc-U0Ovs0nyzJjlrmnkqsHyeucOfdUcvMgeSuKqyiG_7a4DxRRz-d9OZ1VmvikUJxGh8oNGEmxd1Oz8BJVCMa1aE1nEiqnses5oQqBVETv0CRGf3FojaP51j9QjwJ8n-6t5aRagkqO4vnNPhIH75j-KfVTyeVYCyDd0vJ72PexoS_RN_M1y3iYfbOgidNC5TLs3YLGenzxNwAA__9IF1PL"
	if url.String() != expectedURL {
		t.Errorf("expected `%s` got `%s`", expectedURL, url.String())
	}
}
