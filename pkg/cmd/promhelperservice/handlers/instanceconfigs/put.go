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
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/logging"
)

// PutHandler handles PUT /instance-configs/{cluster_id}
// Doc reference - https://cockroachlabs.atlassian.net/wiki/x/MAAlzg
func PutHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	log := logging.MakeLogger(ctx, "create instance config")
	clusterID := r.PathValue(ClusterIDParam)
	if clusterID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(formFailureResponse("", "could not extract cluster id from URL"))
		return
	}
	requestBody, err := getConfig(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(formFailureResponse(clusterID, "invalid/empty request"))
		log.Errorf("parse request failed: %v\n", err)
		return
	}
	if err = requestBody.validate(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(formFailureResponse(clusterID, fmt.Sprintf("invalid request: %v", err)))
		log.Errorf("request validation failure: %v\n", err)
		return
	}
	if err = createClusterConfigFile(clusterID, requestBody, log); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(formFailureResponse(clusterID, "error creating cluster config"))
		log.Errorf("cluster config creation failed: %v\n", err)
		return
	}
	res, err := generateInstanceConfigResponse(&instanceConfigResponse{
		ClusterID: clusterID,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(formFailureResponse(clusterID, "error generating response"))
		log.Errorf("parse request failed: %v\n", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(res)
}

// createClusterConfigFile creates the cluster config file per node
func createClusterConfigFile(clusterID string, r *instanceConfig, log logging.Logger) error {
	fileName := fmt.Sprintf("%s/%s.%s", instanceConfigDir,
		clusterID, instanceConfigFileExt)
	cc, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer func() { _ = cc.Close() }()
	_, _ = cc.Write([]byte(r.Config))
	log.Infof("File created: %v", fileName)
	return nil
}
