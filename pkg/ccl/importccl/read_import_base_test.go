// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestRejectedFilename(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name     string
		fname    string
		rejected string
	}{
		{
			name:     "http",
			fname:    "http://127.0.0.1",
			rejected: "http://127.0.0.1/.rejected",
		},
		{
			name:     "nodelocal",
			fname:    "nodelocal:///file.csv",
			rejected: "nodelocal:///file.csv.rejected",
		},
	}
	for _, tc := range tests {
		rej, err := rejectedFilename(tc.fname)
		if err != nil {
			t.Fatal(err)
		}
		if tc.rejected != rej {
			t.Errorf("expected:\n%v\ngot:\n%v\n", tc.rejected, rej)
		}
	}
}
