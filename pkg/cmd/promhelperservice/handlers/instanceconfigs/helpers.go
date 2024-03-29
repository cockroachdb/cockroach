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
	"flag"
	"fmt"
	"io"
)

const (
	// resourceName and version for the API
	resourceName    = "instance-configs"
	resourceVersion = "v1"
	// ClusterIDParam is the url parameter
	ClusterIDParam = "cluster_id"
	// defaultInstanceConfigDir is the default instance config directory from where the yml files are created/removed.
	defaultInstanceConfigDir = "/opt/prom/prometheus/instance-configs"

	instanceConfigFileExt = "yml"
)

var instanceConfigDir = "/opt/prom/prometheus/instance-configs"

// BuildInstanceConfigUrl returns the url with cluster ID for instance config
func BuildInstanceConfigUrl() string {
	return fmt.Sprintf("/%s/%s/{%s}", resourceVersion, resourceName, ClusterIDParam)
}

// RegisterFlag registers the flags needed by the handler at the application start
func RegisterFlag() {
	flag.StringVar(&instanceConfigDir, "instance-config-dir", defaultInstanceConfigDir,
		"intance config directory for prometheus")
}

// instanceConfig is the HTTP request received for generating instance config
type instanceConfig struct {
	Config string `json:"config"`
}

// validate validates the instanceConfig
func (r *instanceConfig) validate() error {
	if r.Config == "" {
		return fmt.Errorf("no config available")
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
