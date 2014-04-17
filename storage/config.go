// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/gossip"
	yaml "gopkg.in/yaml.v1"
)

// Replica describes a replica location by address (host:port), disk
// (device name) and range start key.
type Replica struct {
	Addr     string // host:port.
	Disk     string // e.g. ssd1.
	RangeKey []byte // Range start key.
}

// DiskType is the type of a disk that a Store is storing data on.
type DiskType uint32

const (
	// SSD = Solid State Disk
	SSD DiskType = iota
	// HDD = Spinning disk
	HDD
)

// DiskCapacity contains capacity information for a storage device.
type DiskCapacity struct {
	Capacity  uint64
	Available uint64
}

// NodeConfig holds configuration about the node.
type NodeConfig struct {
	Address    string
	DataCenter string
	PDU        string
	Rack       string
}

// AvailableDiskConfig holds information about a disk that is available in a RoachNode.
type AvailableDiskConfig struct {
	Node         NodeConfig
	Disk         string
	DiskCapacity DiskCapacity
	DiskType     DiskType
}

// ZoneConfig holds configuration that is needed for a range of KV pairs.
type ZoneConfig struct {
	// Replicas is a map from datacenter name to a slice of disk types.
	Replicas      map[string]([]DiskType) `yaml:"replicas,omitempty"`
	RangeMinBytes uint64                  `yaml:"range_min_bytes,omitempty"`
	RangeMaxBytes uint64                  `yaml:"range_max_bytes,omitempty"`
}

// ParseZoneConfig parses a YAML serialized ZoneConfig.
func ParseZoneConfig(in []byte) (*ZoneConfig, error) {
	z := &ZoneConfig{}
	err := yaml.Unmarshal(in, z)
	return z, err
}

// ToYAML serializes a ZoneConfig as YAML.
func (z *ZoneConfig) ToYAML() ([]byte, error) {
	return yaml.Marshal(z)
}

// Less compares two AvailableDiskConfigs based on percentage of disk available.
func (a AvailableDiskConfig) Less(b gossip.Ordered) bool {
	return a.DiskCapacity.PercentAvail() < b.(AvailableDiskConfig).DiskCapacity.PercentAvail()
}

// PercentAvail computes the percentage of disk space that is available.
func (d DiskCapacity) PercentAvail() float64 {
	return float64(d.Available) / float64(d.Capacity)
}
