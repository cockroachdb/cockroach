// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/stretchr/testify/require"
)

// ReplicaType represents the type of replica
type ReplicaType string

const (
	// VOTER represents a voter replica
	VOTER ReplicaType = "VOTER"
	// NON_VOTER represents a non-voter replica
	NON_VOTER ReplicaType = "NON_VOTER"
)

// StoreWeight represents a store and its weight
type StoreWeight struct {
	StoreID string
	Ratio   int
}

// ReplicaConfig represents a replica configuration
type ReplicaConfig struct {
	ReplicaType  ReplicaType
	StoreWeights []StoreWeight
}

// LeaseWeight represents a lease and its weight
type LeaseWeight struct {
	LeaseID int
	Weight  float64
}

// Configuration represents the complete configuration
type Configuration struct {
	LeaseWeights   []LeaseWeight
	ReplicaConfigs []ReplicaConfig
}

// Parse parses the configuration string and returns a Configuration struct
func Parse(input string) (Configuration, error) {
	for _, line := range strings.Split(d.Input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		tag, data, found := strings.Cut(line, ":")
		require.True(t, found)
		tag, data = strings.TrimSpace(tag), strings.TrimSpace(data)
		span := spanconfigtestutils.ParseSpan(t, tag)
		conf := spanconfigtestutils.ParseZoneConfig(t, data).AsSpanConfig()
		eventGen.ScheduleEvent(settingsGen.Settings.StartTime, delay, event.SetSpanConfigEvent{
			Span:   span,
			Config: conf,
		})
	}
}
