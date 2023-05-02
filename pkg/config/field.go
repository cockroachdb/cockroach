// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package config contains basic utilities and data definitions for zone
// configuration.
package config

import "github.com/cockroachdb/redact"

// Field corresponds to a field in either a SpanConfig or ZoneConfig.
type Field int

// SafeValue makes Field a SafeValue.
func (i Field) SafeValue() {}

var _ redact.SafeValue = Field(0)

//go:generate stringer --type=Field --linecomment

const (
	_                Field = iota
	RangeMinBytes          // range_min_bytes
	RangeMaxBytes          // range_max_bytes
	GlobalReads            // global_reads
	NumReplicas            // num_replicas
	NumVoters              // num_voters
	GCTTL                  // gc.ttlseconds
	Constraints            // constraints
	VoterConstraints       // voter_constraints
	LeasePreferences       // lease_preferences

	// NumFields is the number of fields in the config.
	NumFields int = iota - 1
)
