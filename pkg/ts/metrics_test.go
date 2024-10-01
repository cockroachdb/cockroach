// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestTimeSeriesWriteMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tm := newTestModelRunner(t)
	tm.Start()
	defer tm.Stop()

	metrics := tm.DB.Metrics()

	tm.storeTimeSeriesData(resolution1ns, []tspb.TimeSeriesData{
		tsd("test.multimetric", "source1",
			tsdp(1, 100),
			tsdp(15, 300),
			tsdp(17, 500),
			tsdp(52, 900),
		),
		tsd("test.multimetric", "source2",
			tsdp(5, 100),
			tsdp(16, 300),
			tsdp(22, 500),
			tsdp(82, 900),
		),
	})
	tm.assertKeyCount(7)
	tm.assertModelCorrect()

	if a, e := metrics.WriteSamples.Count(), int64(8); a != e {
		t.Fatalf("samples written was %d, wanted %d", a, e)
	}

	originalBytes := metrics.WriteBytes.Count()
	if a, e := originalBytes, int64(0); a <= e {
		t.Fatalf("sample bytes written was %d, wanted more than %d", a, e)
	}

	if a, e := metrics.WriteErrors.Count(), int64(0); a != e {
		t.Fatalf("write error count was %d, wanted %d", a, e)
	}

	// Introduce an error into the db.
	if err := tm.DB.StoreData(context.Background(), resolutionInvalid, []tspb.TimeSeriesData{
		{
			Name:   "test.multimetric",
			Source: "source3",
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					Value:          1,
					TimestampNanos: 1,
				},
			},
		},
	}); err == nil {
		t.Fatal("StoreData for invalid resolution did not throw error, wanted an error")
	}

	if a, e := metrics.WriteSamples.Count(), int64(8); a != e {
		t.Fatalf("samples written was %d, wanted %d", a, e)
	}

	if a, e := metrics.WriteBytes.Count(), originalBytes; a != e {
		t.Fatalf("sample bytes written was %d, wanted %d", a, e)
	}

	if a, e := metrics.WriteErrors.Count(), int64(1); a != e {
		t.Fatalf("write error count was %d, wanted %d", a, e)
	}
}
