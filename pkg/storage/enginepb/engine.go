// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb

import (
	"fmt"

	"github.com/cockroachdb/redact"
)

// Type implements the pflag.Value interface.
func (e *EngineType) Type() string { return "string" }

// String implements the pflag.Value interface.
func (e *EngineType) String() string { return redact.StringWithoutMarkers(e) }

// SafeFormat implements the redact.SafeFormatter interface.
func (e *EngineType) SafeFormat(p redact.SafePrinter, _ rune) {
	switch *e {
	case EngineTypeDefault:
		p.SafeString("default")
	case EngineTypePebble:
		p.SafeString("pebble")
	default:
		p.Printf("<unknown engine %d>", int32(*e))
	}
}

// Set implements the pflag.Value interface.
func (e *EngineType) Set(s string) error {
	switch s {
	case "default":
		*e = EngineTypeDefault
	case "pebble":
		*e = EngineTypePebble
	default:
		return fmt.Errorf("invalid storage engine: %s "+
			"(possible values: pebble)", s)
	}
	return nil
}
