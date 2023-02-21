// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grafana

import (
	"embed"
	"os"
)

//go:embed configs/*.json
var configs embed.FS

// GetDashboards returns the grafana dashboards to be installed.
// or if none is specified, the default dashboards.
func GetDashboards(grafanaConfigFile string) ([]string, error) {
	grafanaJSON := make([]string, 0)
	if grafanaConfigFile != "" {
		data, err := os.ReadFile(grafanaConfigFile)
		if err != nil {
			return nil, err
		}
		grafanaJSON = []string{string(data)}
	}
	// Only add the default dashboards if no custom dashboard was specified.
	if len(grafanaJSON) == 0 {
		entries, err := configs.ReadDir("configs")
		if err != nil {
			return nil, err
		}

		for _, v := range entries {
			data, err := configs.ReadFile("configs/" + v.Name())
			if err != nil {
				return nil, err
			}
			grafanaJSON = append(grafanaJSON, string(data))
		}
	}
	return grafanaJSON, nil
}
