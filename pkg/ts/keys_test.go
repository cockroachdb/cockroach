// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDataKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name           string
		source         string
		timestamp      int64
		resolution     Resolution
		expectedLen    int
		expectedPretty string
	}{
		{
			"test.metric",
			"testsource",
			0,
			Resolution10s,
			30,
			"/System/tsd/test.metric/testsource/10s/1970-01-01T00:00:00Z",
		},
		{
			"test.no.source",
			"",
			1429114700000000000,
			Resolution10s,
			26,
			"/System/tsd/test.no.source//10s/2015-04-15T16:00:00Z",
		},
		{
			"",
			"",
			-1429114700000000000,
			Resolution10s,
			12,
			"/System/tsd///10s/1924-09-18T08:00:00Z",
		},
	}

	for i, tc := range testCases {
		encoded := MakeDataKey(tc.name, tc.source, tc.resolution, tc.timestamp)
		if !bytes.HasPrefix(encoded, keys.TimeseriesPrefix) {
			t.Errorf("%d: encoded key %v did not have time series data prefix", i, encoded)
		}
		if a, e := len(encoded), tc.expectedLen; a != e {
			t.Errorf("%d: encoded length %d did not match expected %d", i, a, e)
		}

		// Normalize timestamp of test case; we expect MakeDataKey to
		// automatically truncate it to an exact multiple of the Resolution's
		// KeyDuration
		tc.timestamp = (tc.timestamp / tc.resolution.SlabDuration()) * tc.resolution.SlabDuration()

		d := tc
		var err error
		d.name, d.source, d.resolution, d.timestamp, err = DecodeDataKey(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(d, tc) {
			t.Errorf("%d: decoded values %v did not match expected %v", i, d, tc)
		}
		if pretty := keys.PrettyPrint(nil /* valDirs */, encoded); tc.expectedPretty != pretty {
			t.Errorf("%d: expected %s, but got %s", i, tc.expectedPretty, pretty)
		}
	}
}
