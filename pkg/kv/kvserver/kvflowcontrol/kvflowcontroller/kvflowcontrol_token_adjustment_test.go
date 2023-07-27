// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowcontroller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/datadriven"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func TestFlowTokenAdjustment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		ctx         = context.Background()
		controller  *Controller
		adjustments []adjustment
		stream      = kvflowcontrol.Stream{
			TenantID: roachpb.SystemTenantID,
			StoreID:  roachpb.StoreID(1),
		}
	)

	datadriven.RunTest(t, "testdata/flow_token_adjustment",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				controller = New(
					metric.NewRegistry(),
					cluster.MakeTestingClusterSettings(),
					hlc.NewClockForTesting(nil),
				)
				adjustments = nil
				return ""

			case "adjust":
				require.NotNilf(t, controller, "uninitialized flow controller (did you use 'init'?)")

				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					require.Len(t, parts, 2, "expected form 'class={regular,elastic} delta={+,-}<size>")

					var delta kvflowcontrol.Tokens
					var pri admissionpb.WorkPriority

					// Parse class={regular,elastic}.
					parts[0] = strings.TrimSpace(parts[0])
					require.True(t, strings.HasPrefix(parts[0], "class="))
					parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "class=")
					switch parts[0] {
					case "regular":
						pri = admissionpb.NormalPri
					case "elastic":
						pri = admissionpb.BulkNormalPri
					}

					// Parse delta={+,-}<size>
					parts[1] = strings.TrimSpace(parts[1])
					require.True(t, strings.HasPrefix(parts[1], "delta="))
					parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "delta=")
					require.True(t, strings.HasPrefix(parts[1], "+") || strings.HasPrefix(parts[1], "-"))
					isPositive := strings.Contains(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "-")
					bytes, err := humanize.ParseBytes(parts[1])
					require.NoError(t, err)
					delta = kvflowcontrol.Tokens(int64(bytes))
					if !isPositive {
						delta = -delta
					}

					controller.adjustTokens(ctx, pri, delta, stream)
					adjustments = append(adjustments, adjustment{
						pri:   pri,
						delta: delta,
						post:  controller.testingGetTokensForStream(stream),
					})
				}
				return ""

			case "history":
				limit := controller.testingGetLimit()

				var buf strings.Builder
				buf.WriteString("                   regular |  elastic\n")
				buf.WriteString(fmt.Sprintf("                  %8s | %8s\n",
					printTrimmedTokens(limit[admissionpb.RegularWorkClass]),
					printTrimmedTokens(limit[admissionpb.ElasticWorkClass]),
				))
				buf.WriteString("======================================\n")
				for _, h := range adjustments {
					buf.WriteString(fmt.Sprintf("%s\n", h))
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

type adjustment struct {
	pri   admissionpb.WorkPriority
	delta kvflowcontrol.Tokens
	post  tokensPerWorkClass
}

func printTrimmedTokens(t kvflowcontrol.Tokens) string {
	return strings.ReplaceAll(t.String(), " ", "")
}

func (h adjustment) String() string {
	class := admissionpb.WorkClassFromPri(h.pri)

	comment := ""
	if h.post[admissionpb.RegularWorkClass] <= 0 {
		comment = "regular"
	}
	if h.post[admissionpb.ElasticWorkClass] <= 0 {
		if len(comment) == 0 {
			comment = "elastic"
		} else {
			comment = "regular and elastic"
		}
	}
	if len(comment) != 0 {
		comment = fmt.Sprintf(" (%s blocked)", comment)
	}
	return fmt.Sprintf("%8s %7s  %8s | %8s%s",
		printTrimmedTokens(h.delta),
		class,
		printTrimmedTokens(h.post[admissionpb.RegularWorkClass]),
		printTrimmedTokens(h.post[admissionpb.ElasticWorkClass]),
		comment,
	)
}
