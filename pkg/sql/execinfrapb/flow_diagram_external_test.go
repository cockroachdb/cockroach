// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
		catpb.NewBasePrivilegeDescriptor(username.NodeUserName()),
		nil,
		nil,
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
	expectedURL := "https://cockroachdb.github.io/distsqlplan/decode.html#eJyMk1Fv1DAMx9_5FJWfM9FubECfkNhJnMQY3O0NVVZovF60NAlxyq2c-t1R2k2DCXLkwZL9s_9Oo38PwN8N1LC9vloV2y8fiw-rzQoEWKfok-yJof4KFTQCfHAtMbuQSoe5Ya3uoS4FaOuHmMqNgNYFgvoAUUdDUMON_GZoQ1JReFmCAEVRajPLRuL4LgX0dzSCgPfODL3luog7zagZJf6gMKJxtkMre8KyEhl4moNnAgRk-Kvc8HkOXhxRfp0bfpODb_PKVZkZrnJPVZ0eUT7LDeeeqjo_onwhintRjM-6ePAUUM3xqTnuZEw8VSik1KK0IwaS7GxyFu6D9F7bDo3udURpFfYDR5TekwzoLOrI6PYWjbY0b_0JzSTADfHJtRxlR1BXv9l8fQl1OYn_d_qG2DvL9IfJ_7WpfLbppJoaAaQ6Wn4vdkNo6XNw7dy7pNez0FxQxHGh1ZKs7SPiGEj2y_UbAbfG7VErqKF8OCd_CY8H0oDsOH3Yduf2s-zN6NO1bqVhEnAl7-iSIoVeW81Rtw9kml78CgAA__8xyIKF"
	if url.String() != expectedURL {
		t.Errorf("expected `%s` got `%s`", expectedURL, url.String())
	}
}
