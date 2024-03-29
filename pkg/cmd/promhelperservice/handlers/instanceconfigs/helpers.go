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
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
	// resourceName and version for the API
	resourceName    = "instance-configs"
	resourceVersion = "v1"
	// ClusterIDParam is the url parameter
	ClusterIDParam = "cluster_id"
	// defaultInstanceConfigDir is the default instance config directory from where the yml files are created/removed.
	defaultInstanceConfigDir = "/opt/prom/prometheus/instance-configs"
	instanceConfigFileExt    = "yml"
	// instanceConfigEnvVar needs to be set to override the defaultInstanceConfigDir
	instanceConfigEnvVar = "INSTANCE_CONFIG_DIR"
)

// FormInsConfUrlWithClusterID returns the url with cluster ID for instance config
func FormInsConfUrlWithClusterID() string {
	return fmt.Sprintf("/%s/%s/{%s}", resourceVersion, resourceName, ClusterIDParam)
}

// getDir returns the instance config dir. The value can be read from
// env instanceConfigEnvVar, else the defaultInstanceConfigDir is used.
func getDir() string {
	instanceConfigDir := os.Getenv(instanceConfigEnvVar)
	if instanceConfigDir == "" {
		instanceConfigDir = defaultInstanceConfigDir
	}
	return instanceConfigDir
}

// Node has the configuration of a specific node of a cluster
type Node struct {
	NodeID string `json:"node_id"`
	// Targets are the list of targets in the host:port format
	Targets []string `json:"targets"`
	// Labels are the labels to be added for the node. Note that the "node" label is automatically added from NodeID
	Labels map[string]string `json:"labels"`
}

// instanceConfig is the HTTP request received for generating instance config
type instanceConfig struct {
	Nodes []*Node `json:"nodes"`
}

// validate validates the instanceConfig
func (r *instanceConfig) validate() error {
	if len(r.Nodes) == 0 {
		return fmt.Errorf("no nodes present for instance config")
	}
	for i, n := range r.Nodes {
		if n.NodeID == "" {
			return fmt.Errorf("node_id is missing for node %d", i+1)
		}
		if len(n.Targets) == 0 {
			return fmt.Errorf("no targets present for node %s", n.NodeID)
		}
		for ti, t := range n.Targets {
			if t == "" {
				return fmt.Errorf("target at %d for node %s is blank", ti, n.NodeID)
			}
		}
	}
	return nil
}

// instanceConfigResponse is the response for create instance config
type instanceConfigResponse struct {
	ClusterID     string `json:"cluster_id"`
	FailureReason string `json:"failure_reason,omitempty"`
}

// getConfig returns the instanceConfig after parsing the request json
func getConfig(body io.ReadCloser) (*instanceConfig, error) {
	var insConfigReq instanceConfig
	if err := json.NewDecoder(body).Decode(&insConfigReq); err != nil {
		return nil, err
	}
	return &insConfigReq, nil
}

// generateInstanceConfigResponse returns the instanceConfig after parsing the request json
func generateInstanceConfigResponse(insConfigRes *instanceConfigResponse) ([]byte, error) {
	var res []byte
	var err error
	if res, err = json.Marshal(insConfigRes); err != nil {
		return nil, err
	}
	return res, nil
}

// formFailureResponse forms the failure response in bytes
func formFailureResponse(clusterID, errorMessage string) []byte {
	response, _ := generateInstanceConfigResponse(&instanceConfigResponse{
		ClusterID:     clusterID,
		FailureReason: errorMessage,
	})
	return response
}
