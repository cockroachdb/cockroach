// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestOptionsValidations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, MakeDefaultOptions().ValidateForCreateChangefeed(),
		"Default options should be valid")

	tests := []struct {
		input map[string]string
		err   string
	}{
		{map[string]string{"format": "txt"}, "unknown format"},
		{map[string]string{"initial_scan": "", "no_initial_scan": ""}, "cannot specify both"},
	}

	for _, test := range tests {
		o := MakeStatementOptions(test.input)
		err := o.ValidateForCreateChangefeed()
		require.Error(t, err, "%v should not be valid", test.input)
		require.Contains(t, err.Error(), test.err)
	}

}
