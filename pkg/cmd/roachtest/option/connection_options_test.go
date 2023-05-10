// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package option

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFirstOptionCreatesMap(t *testing.T) {
	var opts ConnOption
	o := ConnectionOption("a", "b")
	o(&opts)
	require.NotNil(t, opts.Options)
}

func TestTimeoutCalculation(t *testing.T) {
	var opts ConnOption
	for _, d := range []struct {
		t time.Duration
		o string
	}{
		{
			t: time.Second,
			o: "1",
		},
		{
			t: time.Millisecond,
			o: "1",
		},
		{
			t: time.Minute,
			o: "60",
		},
	} {
		t.Run(d.t.String(), func(t *testing.T) {
			o := ConnectTimeout(d.t)
			o(&opts)
			require.Equal(t, d.o, opts.Options["connect_timeout"])
		})
	}
}
