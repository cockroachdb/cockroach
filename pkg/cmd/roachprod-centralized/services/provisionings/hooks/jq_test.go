// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateJQ_FlatArray(t *testing.T) {
	data := map[string]interface{}{
		"instances": []interface{}{
			map[string]interface{}{"public_ip": "1.2.3.4"},
			map[string]interface{}{"public_ip": "5.6.7.8"},
		},
	}
	results, err := evaluateJQ(".instances[].public_ip", data)
	require.NoError(t, err)
	assert.Equal(t, []string{"1.2.3.4", "5.6.7.8"}, results)
}

func TestEvaluateJQ_SingleValue(t *testing.T) {
	data := map[string]interface{}{
		"ip": "10.0.0.1",
	}
	results, err := evaluateJQ(".ip", data)
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.1"}, results)
}

func TestEvaluateJQ_NestedPath(t *testing.T) {
	data := map[string]interface{}{
		"network": map[string]interface{}{
			"nodes": []interface{}{
				map[string]interface{}{"addr": "10.0.0.1"},
				map[string]interface{}{"addr": "10.0.0.2"},
			},
		},
	}
	results, err := evaluateJQ(".network.nodes[].addr", data)
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, results)
}

func TestEvaluateJQ_MissingPath(t *testing.T) {
	data := map[string]interface{}{
		"other": "value",
	}
	_, err := evaluateJQ(".instances[].public_ip", data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "jq expression")
}

func TestEvaluateJQ_NonStringSkipped(t *testing.T) {
	data := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{"val": "keep"},
			map[string]interface{}{"val": float64(42)},
			map[string]interface{}{"val": "also-keep"},
		},
	}
	results, err := evaluateJQ(".items[].val", data)
	require.NoError(t, err)
	assert.Equal(t, []string{"keep", "also-keep"}, results)
}

func TestEvaluateJQ_InvalidExpression(t *testing.T) {
	data := map[string]interface{}{}
	_, err := evaluateJQ(".[invalid", data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse jq expression")
}

func TestEvaluateJQ_NullResult(t *testing.T) {
	data := map[string]interface{}{
		"a": nil,
	}
	_, err := evaluateJQ(".a", data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no results")
}

func TestEvaluateJQ_EmptyData(t *testing.T) {
	data := map[string]interface{}{}
	_, err := evaluateJQ(".missing", data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no results")
}
