package changefeedccl

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/stretchr/testify/require"
)

func TestConfigAttributes(t *testing.T) {
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
