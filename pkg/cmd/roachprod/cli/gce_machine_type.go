// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

type gceMachineTypeSpec struct {
	MachineType string
	Nodes       int
}

func parseGceMachineTypeSpecs(rawSpecs []string, totalNodes int) ([]gceMachineTypeSpec, error) {
	if len(rawSpecs) == 0 {
		return nil, errors.New("gce machine type must be specified at least once")
	}
	if totalNodes <= 0 {
		return nil, errors.Newf("invalid total node count %d", totalNodes)
	}

	var specs []gceMachineTypeSpec
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

			nodes := 0
			if len(parts) == 2 {
				count := strings.TrimSpace(parts[1])
				if count == "" {
					return nil, errors.Newf("missing node count in %q", spec)
				}
				parsed, err := strconv.Atoi(count)
				if err != nil || parsed <= 0 {
					return nil, errors.Newf("invalid node count %q in %q", count, spec)
				}
				nodes = parsed
			} else {
				hasMissingCount = true
			}

			specs = append(specs, gceMachineTypeSpec{
				MachineType: machineType,
				Nodes:       nodes,
			})
		}
	}

	if len(specs) == 0 {
		return nil, errors.New("gce machine type must be specified at least once")
	}

	if hasMissingCount {
		if len(specs) != 1 {
			return nil, errors.New("machine type counts are required when specifying multiple entries")
		}
		specs[0].Nodes = totalNodes
		return specs, nil
	}

	total := 0
	for _, spec := range specs {
		total += spec.Nodes
	}
	if total != totalNodes {
		return nil, errors.Newf("gce machine type specs cover %d nodes, expected %d", total, totalNodes)
	}

	return specs, nil
}
