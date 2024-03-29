// Copyright 2024 The Cockroach Authors.

// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instanceconfigs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPutHandler(t *testing.T) {
	instanceConfigDir = fmt.Sprintf("%s/%s", t.TempDir(), "instance-configs")
	require.Nil(t, os.Mkdir(instanceConfigDir, 0755))
	t.Run("invalid request", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut,
			fmt.Sprintf("/%s/%s/%s", resourceVersion, resourceName, "c1"),
			nil)
		req.SetPathValue(ClusterIDParam, "c1")
		PutHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusBadRequest, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Equal(t, "c1", insConfigRes.ClusterID)
		require.Contains(t, "invalid/empty request", insConfigRes.FailureReason)
	})
	t.Run("invalid request with missing cluster id", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut,
			fmt.Sprintf("/%s/%s/%s", resourceVersion, resourceName, "c1"),
			bytes.NewReader([]byte("{}")))
		PutHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusBadRequest, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Empty(t, insConfigRes.ClusterID)
		require.Contains(t, "could not extract cluster id from URL", insConfigRes.FailureReason)
	})
	t.Run("invalid request with missing config", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut,
			fmt.Sprintf("/%s/%s/%s", resourceVersion, resourceName, "c1"),
			bytes.NewReader([]byte(`{}`)))
		req.SetPathValue(ClusterIDParam, "c1")
		PutHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusBadRequest, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Equal(t, "c1", insConfigRes.ClusterID)
		require.Contains(t, "invalid request: no config available", insConfigRes.FailureReason)
	})
	t.Run("valid request", func(t *testing.T) {
		clusterName := fmt.Sprintf("test-cluster_%d", time.Now().UnixNano())
		fileName := fmt.Sprintf("%s/%s.%s", instanceConfigDir, clusterName, instanceConfigFileExt)
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut,
			fmt.Sprintf("/%s/%s/%s", resourceVersion, resourceName, clusterName),
			bytes.NewReader([]byte(`{"config": "config content"}`)))
		req.SetPathValue(ClusterIDParam, clusterName)
		PutHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusOK, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Equal(t, clusterName, insConfigRes.ClusterID)
		require.Empty(t, insConfigRes.FailureReason)
		_, err := os.Stat(fileName)
		require.Nil(t, err)
		b, e := os.ReadFile(fileName)
		require.Nil(t, e)
		require.Equal(t, `config content`, string(b))
	})
}
