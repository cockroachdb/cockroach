// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
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
		descpb.NewBasePrivilegeDescriptor(security.NodeUserName()),
	)
	if err != nil {
		t.Fatal(err)
	}

	flows := make(map[roachpb.NodeID]*execinfrapb.FlowSpec)

	tr := execinfrapb.TableReaderSpec{
		Table:     *desc.TableDesc(),
		IndexIdx:  0,
		ColumnIDs: []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
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
	expectedURL := "https://cockroachdb.github.io/distsqlplan/decode.html#eJyMk0Gr2zAQhO_9FWbPglpJk7Y-FUqggbZpk96KWVRrcQSypGrlJm7wfy-2D6EPnvIuhp1vZwTL-Ab820IFp8OXXXH6_rn4tDvuQIDzmr6qjhiqnyChFhCib4jZx0m6zQt7fYWqFGBc6NMk1wIaHwmqGySTLEEFP9QvS0dSmuLrEgRoSsrYOTYRpw8hmk7FAQR89LbvHFdFOhtGw6jwD8UBrXctOtURllJk4CoH1wIEZPibnHmTg9sHyW9z5nc5-D6fLMuMWeZOJVcPktc5c-5UcvMgeSuKqyiGJ1vcB4qo5-99OZ1VmvikUJxGh8oNGEmxd1Ov8BJVCMa1aE1nEiqnses5oQqBVETv0CRGf3FojaP51b9QjwJ8n-6d5aRagkqO4uW9PhIH75j-q_RzyeVYCyDd0vLvsO9jQ9-ib-ZnlvEw-2ZBE6eFymXYuwWN9fjqXwAAAP__T4VS9w=="
	if url.String() != expectedURL {
		t.Errorf("expected `%s` got `%s`", expectedURL, url.String())
	}
}
