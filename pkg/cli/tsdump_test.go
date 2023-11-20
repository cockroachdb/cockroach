// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestDebugTimeSeriesDumpCmd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()
	t.Run("debug tsdump --format=openmetrics", func(t *testing.T) {
		out, err := c.RunWithCapture("debug tsdump --format=openmetrics --cluster-name=test-cluster-1 --disable-cluster-name-verification")
		require.NoError(t, err)
		results := strings.Split(out, "\n")[1:] // Drop first item that contains executed command string.
		require.Equal(t, results[len(results)-1], "", "expected last string to be empty (ends with /\n)")
		require.Equal(t, results[len(results)-2], "# EOF")
		require.Greater(t, len(results), 0)
		require.Greater(t, len(results[:len(results)-2]), 0, "expected to have at least one metric")
	})
}

func TestMakeOpenMetricsWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var out bytes.Buffer
	dataPointsNum := 100
	data := makeTS("cr.test.metric", "source", dataPointsNum)
	w := makeOpenMetricsWriter(&out)
	err := w.Emit(data)
	require.NoError(t, err)
	err = w.Flush()
	require.NoError(t, err)

	var res []string
	for {
		s, err := out.ReadString('\n')
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
		res = append(res, s)
	}
	require.Equal(t, dataPointsNum+1 /* datapoints + EOF final line */, len(res))
}

func makeTS(name, source string, dataPointsNum int) *tspb.TimeSeriesData {
	dps := make([]tspb.TimeSeriesDatapoint, dataPointsNum)
	for i := range dps {
		dps[i] = tspb.TimeSeriesDatapoint{
			TimestampNanos: timeutil.Now().UnixNano(),
			Value:          rand.Float64(),
		}
	}
	return &tspb.TimeSeriesData{
		Name:       name,
		Source:     source,
		Datapoints: dps,
	}
}
