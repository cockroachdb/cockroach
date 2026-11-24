// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// MachineTypeSpec describes a machine type and the number of nodes that should
// use it.
type MachineTypeSpec struct {
	MachineType string
	Count       int
}

// ParseMachineTypeSpecs parses raw --gce-machine-type flag values into a
// validated list of MachineTypeSpec entries. Each raw value may be a single
// machine type (e.g. "n2-standard-16"), a machine type with an explicit node
// count (e.g. "n2-standard-16=4"), or a comma-separated list thereof.
//
// When a single entry is provided without an explicit count, totalNodes is used
// as the count. When multiple entries are provided, each must specify a count
// and the sum must equal totalNodes.
func ParseMachineTypeSpecs(rawSpecs []string, totalNodes int) ([]MachineTypeSpec, error) {
	if len(rawSpecs) == 0 {
		return nil, errors.New("gce machine type must be specified at least once")
	}
	if totalNodes <= 0 {
		return nil, errors.Newf("invalid total node count %d", totalNodes)
	}

	var specs []MachineTypeSpec
	hasMissingCount := false
	for _, rawSpec := range rawSpecs {
		for _, entry := range strings.Split(rawSpec, ",") {
			spec := strings.TrimSpace(entry)
			if spec == "" {
				return nil, errors.New("gce machine type entry cannot be empty")
			}

			parts := strings.SplitN(spec, "=", 2)
			machineType := strings.TrimSpace(parts[0])
			if machineType == "" {
				return nil, errors.Newf("invalid machine type entry %q", spec)
			}

			count := 0
			if len(parts) == 2 {
				countStr := strings.TrimSpace(parts[1])
				if countStr == "" {
					return nil, errors.Newf("missing node count in %q", spec)
				}
				parsed, err := strconv.Atoi(countStr)
				if err != nil || parsed <= 0 {
					return nil, errors.Newf("invalid node count %q in %q", countStr, spec)
				}
				count = parsed
			} else {
				hasMissingCount = true
			}

			specs = append(specs, MachineTypeSpec{
				MachineType: machineType,
				Count:       count,
			})
		}
	}

	if len(specs) == 0 {
		return nil, errors.New("gce machine type must be specified at least once")
	}

	if hasMissingCount {
		if len(specs) != 1 {
			return nil, errors.New(
				"machine type counts are required when specifying multiple entries",
			)
		}
		specs[0].Count = totalNodes
		return specs, nil
	}

	total := 0
	for _, spec := range specs {
		total += spec.Count
	}
	if total != totalNodes {
		return nil, errors.Newf(
			"gce machine type specs cover %d nodes, expected %d", total, totalNodes,
		)
	}

	return specs, nil
}
