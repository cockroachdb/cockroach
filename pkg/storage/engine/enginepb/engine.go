// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package enginepb

import "fmt"

// Type implements the pflag.Value interface.
func (e *EngineType) Type() string { return "string" }

// String implements the pflag.Value interface.
func (e *EngineType) String() string {
	switch *e {
	case EngineTypeDefault:
		return "default"
	case EngineTypeRocksDB:
		return "rocksdb"
	case EngineTypePebble:
		return "pebble"
	case EngineTypeTeePebbleRocksDB:
		return "pebble+rocksdb"
	}
	return ""
}

// Set implements the pflag.Value interface.
func (e *EngineType) Set(s string) error {
	switch s {
	case "default":
		*e = EngineTypeDefault
	case "rocksdb":
		*e = EngineTypeRocksDB
	case "pebble":
		*e = EngineTypePebble
	case "pebble+rocksdb":
		*e = EngineTypeTeePebbleRocksDB
	default:
		return fmt.Errorf("invalid storage engine: %s "+
			"(possible values: rocksdb, pebble)", s)
	}
	return nil
}
