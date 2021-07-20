// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package denylist

import "strings"

//go:generate mockgen -package=denylist -destination=mocks_generated.go -source=service.go . Service

// Entry records the reason for putting an item on the denylist.
// TODO(spaskob): add codes for different denial reasons.
type Entry struct {
	Reason string
}

// DenyEntity represent one denied entity.
// This also serves as the spec for the config format.
type DenyEntity struct {
	Item string `yaml:"item"`
	Type Type   `yaml:"type"`
}

// Type is the type of the denied entity.
type Type int

// Enum values for Type.
const (
	IPAddrType Type = iota + 1
	ClusterType
	UnknownType
)

var strToTypeMap = map[string]Type{
	"ip":      IPAddrType,
	"cluster": ClusterType,
}

var typeToStrMap = map[Type]string{
	IPAddrType:  "ip",
	ClusterType: "cluster",
}

// UnmarshalYAML implements yaml.Unmarshaler interface for type.
func (typ *Type) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var raw string
	err := unmarshal(&raw)
	if err != nil {
		return err
	}

	normalized := strings.ToLower(raw)
	t, ok := strToTypeMap[normalized]
	if !ok {
		*typ = UnknownType
	} else {
		*typ = t
	}

	return nil
}

// MarshalYAML implements yaml.Marshaler interface for type.
func (typ Type) MarshalYAML() (interface{}, error) {
	return typ.String(), nil
}

// String implements Stringer interface for type.
func (typ Type) String() string {
	s, ok := typeToStrMap[typ]
	if !ok {
		return "UNKNOWN"
	}
	return s
}

// Service provides an interface for checking if an id has been denied access.
type Service interface {
	// Denied returns a non-nil Entry if the id is denied. The reason for the
	// denial will be in Entry.
	Denied(entity DenyEntity) (*Entry, error)

	// TODO(spaskob): add API for registering listeners to be notified of any
	// updates (inclusion/exclusion) to the denylist.
}
