// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/datadriven"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func TestAdjustBurstCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		iogc *IOGrantCoordinator
		hist []histItem
		ctx  = context.Background()

		ts = TenantStoreTuple{
			roachpb.SystemTenantID,
			roachpb.StoreID(1),
		}
	)

	datadriven.RunTest(t, "testdata/adjust_burst_capacity",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				iogc = newIOGrantCoordinator(metric.NewRegistry())
				hist = nil
				return ""

			case "adjust":
				require.NotNilf(t, iogc, "uninitialized io grant coordinator (did you use 'init'?)")

				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					require.Len(t, parts, 2, "expected form 'class={regular,elastic} delta={+,-}<int>MB")

					var delta int64
					var pri admissionpb.WorkPriority

					parts[0] = strings.TrimSpace(parts[0])
					require.True(t, strings.HasPrefix(parts[0], "class="))
					parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "class=")
					switch parts[0] {
					case "regular":
						pri = admissionpb.NormalPri
					case "elastic":
						pri = admissionpb.BulkNormalPri
					}

					parts[1] = strings.TrimSpace(parts[1])
					require.True(t, strings.HasPrefix(parts[1], "delta="))
					parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "delta=")
					require.True(t, strings.HasPrefix(parts[1], "+") || strings.HasPrefix(parts[1], "-"))
					isPositive := strings.Contains(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "-")
					bytes, err := humanize.ParseBytes(parts[1])
					require.NoError(t, err)
					delta = int64(bytes)
					if !isPositive {
						delta = -delta
					}

					iogc.AdjustBurstCapacity(ctx, ts, pri, delta, TermIndexTuple{})
					hist = append(hist, histItem{
						adjustedPri:            pri,
						adjustedDelta:          delta,
						postAdjustmentCapacity: iogc.TestingGetWriteBurstCapacity(ts),
					})
				}
				return ""

			case "history":
				var buf strings.Builder
				buf.WriteString("                   regular |  elastic\n")
				buf.WriteString(fmt.Sprintf("                  %8s | %8s\n",
					printBytes(iogc.maxBurstCapacity[regularWorkClass]),
					printBytes(iogc.maxBurstCapacity[elasticWorkClass]),
				))
				buf.WriteString("======================================\n")
				for _, h := range hist {
					buf.WriteString(fmt.Sprintf("%s\n", h))
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

type histItem struct {
	adjustedPri            admissionpb.WorkPriority
	adjustedDelta          int64
	postAdjustmentCapacity [numWorkClasses]int64
}

func printBytes(b int64) string {
	sign := "+"
	if b < 0 {
		sign = "-"
		b = -b
	}
	return fmt.Sprintf("%s%s",
		sign, strings.ReplaceAll(humanize.IBytes(uint64(b)), " ", ""))
}
func (h histItem) String() string {
	class := workClassFromPri(h.adjustedPri)

	comment := ""
	if h.postAdjustmentCapacity[regularWorkClass] <= 0 {
		comment = "regular"
	}
	if h.postAdjustmentCapacity[elasticWorkClass] <= 0 {
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
		printBytes(h.adjustedDelta),
		class,
		printBytes(h.postAdjustmentCapacity[regularWorkClass]),
		printBytes(h.postAdjustmentCapacity[elasticWorkClass]),
		comment,
	)
}
