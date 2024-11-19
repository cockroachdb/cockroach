// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func scanFloat(t *testing.T, d *datadriven.TestData, key string) float64 {
	var vstring string
	d.ScanArgs(t, key, &vstring)
	v, err := strconv.ParseFloat(vstring, 64)
	require.NoError(t, err)
	return v
}

func printLinearModel(b *strings.Builder, m tokensLinearModel) {
	fmt.Fprintf(b, "%.2fx+%d", m.multiplier, m.constant)
}

func printLinearModelFitter(b *strings.Builder, fitter tokensLinearModelFitter) {
	fmt.Fprintf(b, "int: ")
	printLinearModel(b, fitter.intLinearModel)
	fmt.Fprintf(b, " smoothed: ")
	printLinearModel(b, fitter.smoothedLinearModel)
	fmt.Fprintf(b, " per-work-accounted: %d\n", fitter.smoothedPerWorkAccountedBytes)
}

func TestTokensLinearModelFitter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var fitter tokensLinearModelFitter
	fitterToString := func() string {
		var b strings.Builder
		printLinearModelFitter(&b, fitter)
		return b.String()
	}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "tokens_linear_model_fitter"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				multMin := scanFloat(t, d, "mult-min")
				multMax := scanFloat(t, d, "mult-max")
				updateWithZeroActualNonZeroAccounted := false
				if d.HasArg("ingested-model") {
					d.ScanArgs(t, "ingested-model", &updateWithZeroActualNonZeroAccounted)
				}
				fitter = makeTokensLinearModelFitter(multMin, multMax, updateWithZeroActualNonZeroAccounted)
				return fitterToString()

			case "update":
				var accountedBytes, actualBytes, workCount int
				d.ScanArgs(t, "accounted-bytes", &accountedBytes)
				d.ScanArgs(t, "actual-bytes", &actualBytes)
				d.ScanArgs(t, "work-count", &workCount)
				fitter.updateModelUsingIntervalStats(
					int64(accountedBytes), int64(actualBytes), int64(workCount))
				return fitterToString()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
