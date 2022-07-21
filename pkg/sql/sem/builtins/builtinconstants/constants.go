// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtinconstants

import "time"

// SequenceNameArg represents the name of sequence (string) arguments in
// builtin functions.
// Namely, it exists to classify overloads of functions which in postgres
// only take `REGCLASS`, but in cockroach db take both `REGCLASS` and
// `STRING` because we need to be backwards compatible.
const SequenceNameArg = "sequence_name"

// DefaultFollowerReadDuration represents the default time span back from the
// statement time which we wish to be recent and old enough for a follower read.
// Such a default will be returned if we do *not* have an enterprise license
// on a CCL distribution, which may not result in reading from the nearest replica.
const DefaultFollowerReadDuration = -4800 * time.Millisecond

// MaxAllocatedStringSize represents the maximum allowed string length
// in various string related builtin function.
const MaxAllocatedStringSize = 128 * 1024 * 1024

// ErrInsufficientArgsFmtString represents illegal or unknown argument(s) to
// builtin functions.
const ErrInsufficientArgsFmtString = "unknown signature: %s()"

// The following constants are used to categorize builtin functions
// for documentation.
const (
	CategoryArray               = "Array"
	CategoryComparison          = "Comparison"
	CategoryCompatibility       = "Compatibility"
	CategoryCrypto              = "Cryptographic"
	CategoryDateAndTime         = "Date and time"
	CategoryEnum                = "Enum"
	CategoryFullTextSearch      = "Full Text Search"
	CategoryGenerator           = "Set-returning"
	CategoryTrigram             = "Trigrams"
	CategoryFuzzyStringMatching = "Fuzzy String Matching"
	CategoryIDGeneration        = "ID generation"
	CategoryJSON                = "JSONB"
	CategoryMultiRegion         = "Multi-region"
	CategoryMultiTenancy        = "Multi-tenancy"
	CategorySequences           = "Sequence"
	CategorySpatial             = "Spatial"
	CategoryString              = "String and byte"
	CategorySystemInfo          = "System info"
	CategorySystemRepair        = "System repair"
	CategoryStreamIngestion     = "Stream Ingestion"
)

const (
	// GatewayRegionBuiltinName is the name for the builtin that returns the gateway
	// region of the current node.
	GatewayRegionBuiltinName = "gateway_region"
	// DefaultToDatabasePrimaryRegionBuiltinName is the name for the builtin that
	// takes in a region and returns it if it is a valid region on the database.
	// Otherwise, it returns the primary region.
	DefaultToDatabasePrimaryRegionBuiltinName = "default_to_database_primary_region"
	// RehomeRowBuiltinName is the name for the builtin that rehomes a row to the
	// user's gateway region, defaulting to the database primary region.
	RehomeRowBuiltinName = "rehome_row"
	// CreateSchemaTelemetryJobBuiltinName is the name for the builtin that
	// creates a job that logs SQL schema telemetry.
	CreateSchemaTelemetryJobBuiltinName = "crdb_internal.create_sql_schema_telemetry_job"
)

// NodeIDBits is the number of bits stored in the lower portion of
// GenerateUniqueInt.
const NodeIDBits = 15
