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
	"net"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
	yaml "gopkg.in/yaml.v1"
)

const (
	// SSD = Solid State Disk
	SSD DiskType = iota
	// HDD = Spinning disk
	HDD
	// MEM = DRAM
	MEM
)

// DiskType is the type of a disk that a Store is storing data on.
type DiskType uint32

// StringToDiskType converts disk type string to DiskType. Returns
// "HDD" as default if no matching disk type is found.
func StringToDiskType(str string) DiskType {
	switch str {
	case "SSD":
		return SSD
	case "HDD":
		return HDD
	case "MEM":
		return MEM
	default:
		glog.Errorf("invalid disk type specified %q; using HDD", str)
		return HDD
	}
}

// Replica describes a replica location by node ID (corresponds to a
// host:port via lookup on gossip network), store ID (corresponds to
// a physical device, unique per node) and range ID. Datacenter and
// DiskType are provided to optimize reads. Replicas are stored in
// Range lookup records (meta1, meta2).
type Replica struct {
	NodeID     int32
	StoreID    int32
	RangeID    int64
	Datacenter string
	DiskType
}

// StoreCapacity contains capacity information for a storage device.
type StoreCapacity struct {
	Capacity  int64
	Available int64
	DiskType  DiskType
}

// NodeAttributes holds details on node physical/network topology.
type NodeAttributes struct {
	NodeID     int32
	Address    net.Addr
	Datacenter string
	PDU        string
	Rack       string
}

// StoreAttributes holds store information including physical/network
// topology via NodeAttributes and disk type & capacity data.
type StoreAttributes struct {
	StoreID    int32
	Attributes NodeAttributes
	Capacity   StoreCapacity
}

// AcctConfig holds accounting configuration.
type AcctConfig struct {
	// Nothing for the moment.
}

// Permission specifies read/write access and associated priority.
type Permission struct {
	Users    []string `yaml:"users,omitempty"`    // Empty to specify default permission
	Read     bool     `yaml:"read,omitempty"`     // Default means reads are restricted
	Write    bool     `yaml:"write,omitempty"`    // Default means writes are restricted
	Priority float32  `yaml:"priority,omitempty"` // 0.0 means default priority
}

// PermConfig holds permission configuration.
type PermConfig struct {
	Perms []Permission `yaml:"permissions,omitempty"`
}

// ZoneConfig holds configuration that is needed for a range of KV pairs.
type ZoneConfig struct {
	// Replicas is a map from datacenter name to a slice of disk types.
	Replicas      map[string][]string `yaml:"replicas,omitempty"`
	RangeMinBytes int64               `yaml:"range_min_bytes,omitempty"`
	RangeMaxBytes int64               `yaml:"range_max_bytes,omitempty"`
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

// Less compares two StoreAttributess based on percentage of disk available.
func (a StoreAttributes) Less(b gossip.Ordered) bool {
	return a.Capacity.PercentAvail() < b.(StoreAttributes).Capacity.PercentAvail()
}

// PercentAvail computes the percentage of disk space that is available.
func (sc StoreCapacity) PercentAvail() float64 {
	return float64(sc.Available) / float64(sc.Capacity)
}

// RangeLocations is the metadata value stored for a metadata key.
// The metadata key has meta1 or meta2 key prefix and the suffix encodes
// the end key of the range this struct represents.
type RangeLocations struct {
	// The start key of the range represented by this struct, along with the
	// meta1 or meta2 key prefix.
	StartKey Key
	Replicas []Replica
}

// ChooseRandomReplica returns a replica selected at random or nil if none exist.
func ChooseRandomReplica(replicas []Replica) *Replica {
	if len(replicas) == 0 {
		return nil
	}
	r := util.CachedRand
	return &replicas[r.Intn(len(replicas))]
}
