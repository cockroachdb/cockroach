// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
