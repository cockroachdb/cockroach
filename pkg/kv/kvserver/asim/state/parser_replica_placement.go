// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TODO(wenyihu6): add more tests

type StoreWeight struct {
	StoreID StoreID
	Weight  float64
}

// ReplicaConfig represents a replica configuration with its type and store weights
type ReplicaConfig struct {
	ReplicaType  roachpb.ReplicaType
	StoreWeights []StoreWeight
}

// LeaseConfig represents lease weights for stores
type LeaseConfig struct {
	ReplicaIdx int
}

// Configuration represents the complete configuration for replicas and leases
type Configuration struct {
	LeaseWeights   LeaseConfig
	ReplicaConfigs []ReplicaConfig
}

func (c Configuration) String() string {
	buf := strings.Builder{}
	buf.WriteString(fmt.Sprintf("lease: %d\n", c.LeaseWeights.ReplicaIdx))
	for i, replica := range c.ReplicaConfigs {
		buf.WriteString(fmt.Sprintf("replica%d: store_weights=%v replica_type=%s\n", i+1, replica.StoreWeights, replica.ReplicaType))
	}
	return buf.String()
}

// parseStoreWeights parses store weights from a string in the format "[s1=1,s2=2,...]"
// and returns normalized weights that sum to 1.0
func parseStoreWeights(weightsStr string) ([]StoreWeight, error) {
	// Remove surrounding brackets
	weightsStr = strings.Trim(weightsStr, "[]")

	storeWeights := make(map[StoreID]int)
	totalWeight := 0

	// Parse each store=weight pair
	for _, pair := range strings.Split(weightsStr, ",") {
		if pair == "" {
			continue
		}

		key, valStr, found := strings.Cut(pair, "=")
		if !found {
			return nil, fmt.Errorf("invalid weight pair format: %s", pair)
		}

		// Parse store number
		if len(key) < 2 || key[0] != 's' {
			return nil, fmt.Errorf("invalid store identifier: %s", key)
		}
		storeNum, err := strconv.Atoi(key[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid store number: %s", key[1:])
		}

		// Parse weight value
		val, err := strconv.Atoi(valStr)
		if err != nil {
			return nil, fmt.Errorf("invalid weight value: %s", valStr)
		}
		if val < 0 {
			return nil, fmt.Errorf("negative weight not allowed: %d", val)
		}

		storeWeights[StoreID(storeNum)] = val
		totalWeight += val
	}

	if totalWeight == 0 {
		return nil, fmt.Errorf("total weight cannot be zero")
	}

	// Normalize weights
	normalized := make([]StoreWeight, 0)
	for store, weight := range storeWeights {
		normalized = append(normalized, StoreWeight{
			StoreID: store,
			Weight:  float64(weight) / float64(totalWeight),
		})
	}

	return normalized, nil
}

// Parse parses the configuration string and returns a Configuration struct.
// The expected format is:
//
//	lease: r1
//	replica1: store_weights=[s1=1,s2=2,...] replica_type=VOTER
//	replica2: store_weights=[s1=2,s2=1,...] replica_type=NON_VOTER
func Parse(input string) (Configuration, error) {
	var res Configuration

	for i, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Split tag and the rest of the line
		tag, rest, found := strings.Cut(line, ":")
		if !found {
			return Configuration{}, fmt.Errorf("line %d: missing colon", i+1)
		}
		tag = strings.TrimSpace(tag)
		rest = strings.TrimSpace(rest)

		// Split into fields
		fields := strings.Fields(rest)
		if tag == "lease" {
			// Parse lease configuration
			if len(fields) < 1 {
				return Configuration{}, fmt.Errorf("line %d: insufficient fields", i+1)
			}
			// Parse rN field
			if len(fields[0]) < 2 || fields[0][0] != 'r' {
				return Configuration{}, fmt.Errorf("line %d: lease must be specified as 'rN' where N is a number", i+1)
			}
			replicaNum, err := strconv.Atoi(fields[0][1:])
			if err != nil {
				return Configuration{}, fmt.Errorf("line %d: invalid replica number in lease specification", i+1)
			}
			res.LeaseWeights.ReplicaIdx = replicaNum
			continue
		}

		// For replica lines
		if len(fields) < 2 {
			return Configuration{}, fmt.Errorf("line %d: insufficient fields", i+1)
		}

		// Parse weights field
		weightsField := fields[0]
		_, weightsStr, found := strings.Cut(weightsField, "=")
		if !found {
			return Configuration{}, fmt.Errorf("line %d: invalid weights field", i+1)
		}

		weights, err := parseStoreWeights(weightsStr)
		if err != nil {
			return Configuration{}, fmt.Errorf("line %d: %v", i+1, err)
		}

		// Parse replica type
		replicaField := fields[1]
		_, replicaTypeStr, found := strings.Cut(replicaField, "=")
		if !found {
			return Configuration{}, fmt.Errorf("line %d: invalid replica_type field", i+1)
		}
		replicaTypeStr = strings.TrimSpace(replicaTypeStr)

		replicaType, ok := roachpb.ReplicaType_value[replicaTypeStr]
		if !ok {
			return Configuration{}, fmt.Errorf("line %d: invalid replica type: %s", i+1, replicaTypeStr)
		}

		res.ReplicaConfigs = append(res.ReplicaConfigs, ReplicaConfig{
			ReplicaType:  roachpb.ReplicaType(replicaType),
			StoreWeights: weights,
		})
	}

	if len(res.ReplicaConfigs) == 0 {
		return Configuration{}, fmt.Errorf("no replica configurations found")
	}

	return res, nil
}
