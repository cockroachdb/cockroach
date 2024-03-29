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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeleteHandler(t *testing.T) {
	instanceDir := fmt.Sprintf("%s/%s", t.TempDir(), "instance-configs")
	require.Nil(t, os.Setenv(instanceConfigEnvVar, instanceDir))
	require.Nil(t, os.Mkdir(instanceDir, 0755))
	t.Run("invalid request url", func(t *testing.T) {
		clusterName := fmt.Sprintf("test-cluster_%d", time.Now().UnixNano())
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete,
			fmt.Sprintf("/%s/%s/%s", resourceVersion, resourceName, clusterName), nil)
		DeleteHandler(rr, req)
		require.Equal(t, http.StatusBadRequest, rr.Result().StatusCode)
	})
	t.Run("instance config file exists", func(t *testing.T) {
		clusterName := fmt.Sprintf("test-cluster_%d", time.Now().UnixNano())
		fileName := fmt.Sprintf("%s/%s.%s", instanceDir, clusterName, instanceConfigFileExt)
		f, err := os.Create(fileName)
		require.Nil(t, err)
		f.Write([]byte("test-content"))
		f.Close()

		_, err = os.Stat(fileName)
		require.Nil(t, err)
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete,
			fmt.Sprintf("/%s/%s/%s", resourceVersion, resourceName, clusterName), nil)
		req.SetPathValue(ClusterIDParam, clusterName)
		DeleteHandler(rr, req)
		require.Equal(t, http.StatusNoContent, rr.Result().StatusCode)
		_, err = os.Stat(fileName)
		require.NotNil(t, err)
	})
	t.Run("instance config file doesn't exists", func(t *testing.T) {
		clusterName := fmt.Sprintf("test-cluster_%d", time.Now().UnixNano())
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete,
			fmt.Sprintf("/%s/%s/%s", resourceVersion, resourceName, clusterName), nil)
		req.SetPathValue(ClusterIDParam, clusterName)
		DeleteHandler(rr, req)
		require.Equal(t, http.StatusNotFound, rr.Result().StatusCode)
	})
}
