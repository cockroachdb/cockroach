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

// GetDashboardJSONFromFile returns the dashboard JSON from the specified file.
func GetDashboardJSONFromFile(grafanaConfigFile string) (string, error) {
	data, err := os.ReadFile(grafanaConfigFile)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// GetDefaultDashboardJSONs returns the default dashboard JSONs embedded in the
// binary.
func GetDefaultDashboardJSONs() ([]string, error) {
	grafanaJSON := make([]string, 0)
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
	return grafanaJSON, nil
}
