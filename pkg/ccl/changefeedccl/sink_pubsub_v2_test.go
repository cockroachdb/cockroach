// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestConfigAttributes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name               string
		attributes         string
		expectedAttributes pubsubAttributes
		err                string
	}{
		{
			name:               "no attributes",
			expectedAttributes: pubsubAttributes(nil),
		},
		{
			name:       "mixed attributes",
			attributes: `{"attributes": {"foo": "bar", "bar":"foo", "table": "TABLE_NAME"}}`,
			expectedAttributes: map[string]string{
				"foo":   "bar",
				"bar":   "foo",
				"table": "TABLE_NAME",
			},
		},
		{
			name:       "malformed attributes",
			attributes: `{"attributes":{foo: "TABLE_NAME", "bar":"TABLE_NAME}}`,
			err:        "could not marshall",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Test that the config parses correctly.
			cfg, err := unmarshalPubSubConfig(changefeedbase.SinkSpecificJSONConfig(tc.attributes))
			if tc.err != "" {
				require.Errorf(t, err, tc.err)
				return
			} else {
				require.NoError(t, err)
			}
			require.True(t, reflect.DeepEqual(cfg.Attributes, tc.expectedAttributes), fmt.Sprintf("%#v=%#v",
				cfg.Attributes, tc.expectedAttributes))
		})
	}

}
