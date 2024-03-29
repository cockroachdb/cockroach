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

func TestPostHandler(t *testing.T) {
	instanceDir := fmt.Sprintf("%s/%s", t.TempDir(), "instance-configs")
	require.Nil(t, os.Setenv(instanceConfigEnvVar, instanceDir))
	require.Nil(t, os.Mkdir(instanceDir, 0755))
	t.Run("invalid request", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost,
			fmt.Sprintf("/%s/%s", resourceVersion, resourceName), nil)
		PostHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusBadRequest, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Empty(t, insConfigRes.ClusterID)
		require.Equal(t, INVALID, insConfigRes.Result)
		require.Contains(t, "invalid/empty request", insConfigRes.FailureReason)
	})
	t.Run("invalid request with missing cluster id", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost,
			fmt.Sprintf("/%s/%s", resourceVersion, resourceName), bytes.NewReader([]byte("{}")))
		PostHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusBadRequest, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Empty(t, insConfigRes.ClusterID)
		require.Equal(t, INVALID, insConfigRes.Result)
		require.Contains(t, "invalid request: cluster_id is a required field", insConfigRes.FailureReason)
	})
	t.Run("invalid request with missing nodes", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost,
			fmt.Sprintf("/%s/%s", resourceVersion, resourceName), bytes.NewReader([]byte(`{
"cluster_id": "c1"
}`)))
		PostHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusBadRequest, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Empty(t, insConfigRes.ClusterID)
		require.Equal(t, INVALID, insConfigRes.Result)
		require.Contains(t, "invalid request: no nodes present for instance config", insConfigRes.FailureReason)
	})
	t.Run("invalid request with missing node_id", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost,
			fmt.Sprintf("/%s/%s", resourceVersion, resourceName), bytes.NewReader([]byte(`{
"cluster_id": "c1",
"nodes": [{}]
}`)))
		PostHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusBadRequest, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Empty(t, insConfigRes.ClusterID)
		require.Equal(t, INVALID, insConfigRes.Result)
		require.Contains(t, "invalid request: node_id is missing for node 1", insConfigRes.FailureReason)
	})
	t.Run("invalid request with missing targets", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost,
			fmt.Sprintf("/%s/%s", resourceVersion, resourceName), bytes.NewReader([]byte(`{
"cluster_id": "c1",
"nodes": [{"node_id": "1"}]
}`)))
		PostHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusBadRequest, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Empty(t, insConfigRes.ClusterID)
		require.Equal(t, INVALID, insConfigRes.Result)
		require.Contains(t, "invalid request: no targets present for node 1", insConfigRes.FailureReason)
	})
	t.Run("invalid request with blank targets", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost,
			fmt.Sprintf("/%s/%s", resourceVersion, resourceName), bytes.NewReader([]byte(`{
"cluster_id": "c1",
"nodes": [{"node_id": "1", "targets": [""]}]
}`)))
		PostHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusBadRequest, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Empty(t, insConfigRes.ClusterID)
		require.Equal(t, INVALID, insConfigRes.Result)
		require.Contains(t, "invalid request: target at 0 for node 1 is blank", insConfigRes.FailureReason)
	})
	t.Run("valid request", func(t *testing.T) {
		clusterName := fmt.Sprintf("test-cluster_%d", time.Now().UnixNano())
		fileName := fmt.Sprintf("%s/%s.%s", instanceDir, clusterName, instanceConfigFileExt)
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost,
			fmt.Sprintf("/%s/%s", resourceVersion, resourceName), bytes.NewReader([]byte(fmt.Sprintf(`{
"cluster_id": "%s",
"nodes": [{"node_id": "1", "targets": ["t1:900","t2:901"]},
{"node_id": "2", "targets": ["t3:903","t4:904"]}]
}`, clusterName))))
		PostHandler(rr, req)
		result := rr.Result()
		require.Equal(t, http.StatusCreated, result.StatusCode)
		var insConfigRes instanceConfigResponse
		require.Nil(t, json.NewDecoder(result.Body).Decode(&insConfigRes))
		require.Equal(t, clusterName, insConfigRes.ClusterID)
		require.Equal(t, SUCCESS, insConfigRes.Result)
		require.Empty(t, insConfigRes.FailureReason)
		_, err := os.Stat(fileName)
		require.Nil(t, err)
		b, e := os.ReadFile(fileName)
		require.Nil(t, e)
		require.Equal(t, `---
- targets:
  - t1:900
  - t2:901
  labels:
    node: "1"

- targets:
  - t3:903
  - t4:904
  labels:
    node: "2"

`, string(b))
	})
}
