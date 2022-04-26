// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cast

import (
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Context represents the contexts in which a cast can be performed. There
// are three types of cast contexts: explicit, assignment, and implicit. Not all
// casts can be performed in all contexts. See the description of each context
// below for more details.
//
// The concept of cast contexts is taken directly from Postgres's cast behavior.
// More information can be found in the Postgres documentation on type
// conversion: https://www.postgresql.org/docs/current/typeconv.html
type Context uint8

const (
	_ Context = iota
	// ContextExplicit is a cast performed explicitly with the syntax
	// CAST(x AS T) or x::T.
	ContextExplicit
	// ContextAssignment is a cast implicitly performed during an INSERT,
	// UPSERT, or UPDATE statement.
	ContextAssignment
	// ContextImplicit is a cast performed implicitly. For example, the DATE
	// below is implicitly cast to a TIMESTAMPTZ so that the values can be
	// compared.
	//
	//   SELECT '2021-01-10'::DATE < now()
	//
	ContextImplicit
)

// String returns the representation of Context as a string.
func (cc Context) String() string {
	switch cc {
	case ContextExplicit:
		return "explicit"
	case ContextAssignment:
		return "assignment"
	case ContextImplicit:
		return "implicit"
	default:
		return "invalid"
	}
}

// contextOrigin indicates the source of information for a cast's maximum
// context (see cast.MaxContext below). It is only used to annotate entries in
// castMap and to perform assertions on cast entries in the init function. It
// has no effect on the behavior of a cast.
type contextOrigin uint8

const (
	_ contextOrigin = iota
	// contextOriginPgCast specifies that a cast's maximum context is based on
	// information in Postgres's pg_cast table.
	contextOriginPgCast
	// contextOriginAutomaticIOConversion specifies that a cast's maximum
	// context is not included in Postgres's pg_cast table. In Postgres's
	// internals, these casts are evaluated by each data type's input and output
	// functions.
	//
	// Automatic casts can only convert to or from string types [1]. Conversions
	// to string types are assignment casts and conversions from string types
	// are explicit casts [2]. These rules are asserted in the init function.
	//
	// [1] https://www.postgresql.org/docs/13/catalog-pg-cast.html#CATALOG-PG-CAST
	// [2] https://www.postgresql.org/docs/13/sql-createcast.html#SQL-CREATECAST-NOTES
	contextOriginAutomaticIOConversion
	// contextOriginLegacyConversion is used for casts that are not supported by
	// Postgres, but are supported by CockroachDB and continue to be supported
	// for backwards compatibility.
	contextOriginLegacyConversion
)

// Cast includes details about a cast from one OID to another.
//
// TODO(mgartner, otan): Move PerformCast logic to this struct.
type Cast struct {
	// MaxContext is the maximum context in which the cast is allowed. A cast
	// can only be performed in a context that is at or below the specified
	// maximum context.
	//
	// ContextExplicit casts can only be performed in an explicit context.
	//
	// ContextAssignment casts can be performed in an explicit context or in
	// an assignment context in an INSERT, UPSERT, or UPDATE statement.
	//
	// ContextImplicit casts can be performed in any context.
	MaxContext Context
	// origin is the source of truth for the cast's context. It is used to
	// annotate entries in castMap and to perform assertions on cast entries in
	// the init function. It has no effect on the behavior of a cast.
	origin contextOrigin
	// Volatility indicates whether the result of the cast is dependent only on
	// the source value, or dependent on outside factors (such as parameter
	// variables or table contents).
	Volatility volatility.Volatility
	// VolatilityHint is an optional string for Stable casts. When
	// set, it is used as an error hint suggesting a possible workaround when
	// stable casts are not allowed.
	VolatilityHint string
	// intervalStyleAffected is true if the cast is a stable cast when
	// SemaContext.IntervalStyleEnabled is true, and an immutable cast
	// otherwise.
	intervalStyleAffected bool
	// dateStyleAffected is true if the cast is a stable cast when
	// SemaContext.DateStyleEnabled is true, and an immutable cast otherwise.
	dateStyleAffected bool
}

// castMap defines valid casts. It maps from a source OID to a target OID to a
// cast struct that contains information about the cast. Some possible casts,
// such as casts from the UNKNOWN type and casts from a type to the identical
// type, are not defined in the castMap and are instead codified in ValidCast.
//
// Validation is performed on the map in init().
//
// Entries with a contextOriginPgCast origin were automatically generated by the
// cast_map_gen.sh script. The script outputs some types that we do not support.
// Those types were manually deleted. Entries with
// contextOriginAutomaticIOConversion origin were manually added.
var castMap = map[oid.Oid]map[oid.Oid]Cast{
	oid.T_bit: {
		oid.T_bit:    {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int2:   {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:   {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:   {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_varbit: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_bool: {
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_float4:  {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float8:  {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int2:    {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:    {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:    {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_char: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oidext.T_box2d: {
		oidext.T_geometry: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_bpchar: {
		oid.T_bpchar:  {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions from bpchar to other types.
		oid.T_bit:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bool:     {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_box2d: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bytea:    {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_date: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "CHAR to DATE casts depend on session DateStyle; use parse_date(string) instead",
			dateStyleAffected: true,
		},
		oid.T_float4:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_float8:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_geography: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_geometry:  {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_inet:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int2:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_interval: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility:            volatility.Immutable,
			VolatilityHint:        "CHAR to INTERVAL casts depend on session IntervalStyle; use parse_interval(string) instead",
			intervalStyleAffected: true,
		},
		oid.T_jsonb:        {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_numeric:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_record:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regclass:     {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regproc:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regprocedure: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_time: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "CHAR to TIME casts depend on session DateStyle; use parse_time(string) instead",
			dateStyleAffected: true,
		},
		oid.T_timestamp: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: "CHAR to TIMESTAMP casts are context-dependent because of relative timestamp strings " +
				"like 'now' and session settings such as DateStyle; use parse_timestamp(string) instead.",
			dateStyleAffected: true,
		},
		oid.T_timestamptz: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
		},
		oid.T_timetz: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "CHAR to TIMETZ casts depend on session DateStyle; use parse_timetz(char) instead",
			dateStyleAffected: true,
		},
		oid.T_uuid:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varbit: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_void:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_bytea: {
		oidext.T_geography: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oidext.T_geometry:  {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_uuid:         {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		// Casts from BYTEA to string types are stable, since they depend on
		// the bytea_output session variable.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oid.T_char: {
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int4:    {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_name: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		// Automatic I/O conversions from "char" to other types.
		oid.T_bit:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bool:     {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_box2d: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bytea:    {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_date: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    `"char" to DATE casts depend on session DateStyle; use parse_date(string) instead`,
			dateStyleAffected: true,
		},
		oid.T_float4:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_float8:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_geography: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_geometry:  {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_inet:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int2:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_interval: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility:            volatility.Immutable,
			VolatilityHint:        `"char" to INTERVAL casts depend on session IntervalStyle; use parse_interval(string) instead`,
			intervalStyleAffected: true,
		},
		oid.T_jsonb:        {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_numeric:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_record:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regclass:     {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regproc:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regprocedure: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_time: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    `"char" to TIME casts depend on session DateStyle; use parse_time(string) instead`,
			dateStyleAffected: true,
		},
		oid.T_timestamp: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: `"char" to TIMESTAMP casts are context-dependent because of relative timestamp strings ` +
				"like 'now' and session settings such as DateStyle; use parse_timestamp(string) instead.",
			dateStyleAffected: true,
		},
		oid.T_timestamptz: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
		},
		oid.T_timetz: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    `"char" to TIMETZ casts depend on session DateStyle; use parse_timetz(string) instead`,
			dateStyleAffected: true,
		},
		oid.T_uuid:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varbit: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_void:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_date: {
		oid.T_float4:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float8:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int2:        {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:        {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int8:        {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_timestamp:   {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_timestamptz: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: "DATE to CHAR casts are dependent on DateStyle; consider " +
				"using to_char(date) instead.",
			dateStyleAffected: true,
		},
		oid.T_char: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: `DATE to "char" casts are dependent on DateStyle; consider ` +
				"using to_char(date) instead.",
			dateStyleAffected: true,
		},
		oid.T_name: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: "DATE to NAME casts are dependent on DateStyle; consider " +
				"using to_char(date) instead.",
			dateStyleAffected: true,
		},
		oid.T_text: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: "DATE to STRING casts are dependent on DateStyle; consider " +
				"using to_char(date) instead.",
			dateStyleAffected: true,
		},
		oid.T_varchar: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: "DATE to VARCHAR casts are dependent on DateStyle; consider " +
				"using to_char(date) instead.",
			dateStyleAffected: true,
		},
	},
	oid.T_float4: {
		oid.T_bool:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float8:   {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int2:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int4:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_interval: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric:  {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		// Casts from FLOAT4 to string types are stable, since they depend on the
		// extra_float_digits session variable.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oid.T_float8: {
		oid.T_bool:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float4:   {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int2:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int4:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_interval: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric:  {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		// Casts from FLOAT8 to string types are stable, since they depend on the
		// extra_float_digits session variable.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oidext.T_geography: {
		oid.T_bytea:        {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oidext.T_geography: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oidext.T_geometry:  {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_jsonb:        {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oidext.T_geometry: {
		oidext.T_box2d:     {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_bytea:        {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oidext.T_geography: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oidext.T_geometry:  {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_jsonb:        {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_text:         {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_inet: {
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_char: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_int2: {
		oid.T_bit:          {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_bool:         {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_date:         {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float4:       {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_float8:       {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_interval:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:     {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regnamespace: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regproc:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regprocedure: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regrole:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regtype:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_timestamp:    {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_timestamptz:  {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_varbit:       {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_int4: {
		oid.T_bit:          {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_bool:         {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_char:         {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_date:         {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float4:       {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_float8:       {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int2:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_interval:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:     {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regnamespace: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regproc:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regprocedure: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regrole:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regtype:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_timestamp:    {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_timestamptz:  {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_varbit:       {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_int8: {
		oid.T_bit:          {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_bool:         {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_date:         {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float4:       {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_float8:       {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int2:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_interval:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:     {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regnamespace: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regproc:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regprocedure: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regrole:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regtype:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_timestamp:    {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_timestamptz:  {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_varbit:       {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_interval: {
		oid.T_float4:   {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float8:   {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int2:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int8:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_interval: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_numeric:  {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_time:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar: {
			MaxContext:            ContextAssignment,
			origin:                contextOriginAutomaticIOConversion,
			Volatility:            volatility.Immutable,
			VolatilityHint:        "INTERVAL to CHAR casts depend on IntervalStyle; consider using to_char(interval)",
			intervalStyleAffected: true,
		},
		oid.T_char: {
			MaxContext:            ContextAssignment,
			origin:                contextOriginAutomaticIOConversion,
			Volatility:            volatility.Immutable,
			VolatilityHint:        `INTERVAL to "char" casts depend on IntervalStyle; consider using to_char(interval)`,
			intervalStyleAffected: true,
		},
		oid.T_name: {
			MaxContext:            ContextAssignment,
			origin:                contextOriginAutomaticIOConversion,
			Volatility:            volatility.Immutable,
			VolatilityHint:        "INTERVAL to NAME casts depend on IntervalStyle; consider using to_char(interval)",
			intervalStyleAffected: true,
		},
		oid.T_text: {
			MaxContext:            ContextAssignment,
			origin:                contextOriginAutomaticIOConversion,
			Volatility:            volatility.Immutable,
			VolatilityHint:        "INTERVAL to STRING casts depend on IntervalStyle; consider using to_char(interval)",
			intervalStyleAffected: true,
		},
		oid.T_varchar: {
			MaxContext:            ContextAssignment,
			origin:                contextOriginAutomaticIOConversion,
			Volatility:            volatility.Immutable,
			VolatilityHint:        "INTERVAL to VARCHAR casts depend on IntervalStyle; consider using to_char(interval)",
			intervalStyleAffected: true,
		},
	},
	oid.T_jsonb: {
		oid.T_bool:         {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_float4:       {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_float8:       {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oidext.T_geography: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oidext.T_geometry:  {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int2:         {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_numeric:      {MaxContext: ContextExplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_name: {
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_char: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		// Automatic I/O conversions from NAME to other types.
		oid.T_bit:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bool:     {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_box2d: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bytea:    {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_date: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "NAME to DATE casts depend on session DateStyle; use parse_date(string) instead",
			dateStyleAffected: true,
		},
		oid.T_float4:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_float8:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_geography: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_geometry:  {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_inet:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int2:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_interval: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility:            volatility.Immutable,
			VolatilityHint:        "NAME to INTERVAL casts depend on session IntervalStyle; use parse_interval(string) instead",
			intervalStyleAffected: true,
		},
		oid.T_jsonb:        {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_numeric:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_record:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regclass:     {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regproc:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regprocedure: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_time: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "NAME to TIME casts depend on session DateStyle; use parse_time(string) instead",
			dateStyleAffected: true,
		},
		oid.T_timestamp: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: "NAME to TIMESTAMP casts are context-dependent because of relative timestamp strings " +
				"like 'now' and session settings such as DateStyle; use parse_timestamp(string) instead.",
			dateStyleAffected: true,
		},
		oid.T_timestamptz: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
		},
		oid.T_timetz: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "NAME to TIMETZ casts depend on session DateStyle; use parse_timetz(string) instead",
			dateStyleAffected: true,
		},
		oid.T_uuid:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varbit: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_void:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_numeric: {
		oid.T_bool:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float4:   {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_float8:   {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int2:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int4:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_interval: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric:  {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_oid: {
		// TODO(mgartner): Casts to INT2 should not be allowed.
		oid.T_int2:         {MaxContext: ContextAssignment, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:     {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regnamespace: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regproc:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regprocedure: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regrole:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regtype:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_record: {
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oid.T_regclass: {
		// TODO(mgartner): Casts to INT2 should not be allowed.
		oid.T_int2:         {MaxContext: ContextAssignment, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regproc:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regprocedure: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oid.T_regnamespace: {
		// TODO(mgartner): Casts to INT2 should not be allowed.
		oid.T_int2:         {MaxContext: ContextAssignment, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regproc:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regprocedure: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oid.T_regproc: {
		// TODO(mgartner): Casts to INT2 should not be allowed.
		oid.T_int2:         {MaxContext: ContextAssignment, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regprocedure: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oid.T_regprocedure: {
		// TODO(mgartner): Casts to INT2 should not be allowed.
		oid.T_int2:         {MaxContext: ContextAssignment, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regproc:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oid.T_regrole: {
		// TODO(mgartner): Casts to INT2 should not be allowed.
		oid.T_int2:         {MaxContext: ContextAssignment, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regproc:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regprocedure: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oid.T_regtype: {
		// TODO(mgartner): Casts to INT2 should not be allowed.
		oid.T_int2:         {MaxContext: ContextAssignment, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regproc:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regprocedure: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
	},
	oid.T_text: {
		oid.T_bpchar:      {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_char:        {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oidext.T_geometry: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_name:        {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass:    {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Stable},
		// We include a TEXT->TEXT entry to mimic the VARCHAR->VARCHAR entry
		// that is included in the pg_cast table. Postgres doesn't include a
		// TEXT->TEXT entry because it does not allow width-limited TEXT types,
		// so a cast from TEXT->TEXT is always a trivial no-op because the types
		// are always identical (see ValidCast). Because we support
		// width-limited TEXT types with STRING(n), it is possible to have
		// non-identical TEXT types. So, we must include a TEXT->TEXT entry so
		// that casts from STRING(n)->STRING(m) are valid.
		//
		// TODO(#72980): If we use the VARCHAR OID for STRING(n) types rather
		// then the TEXT OID, and we can remove this entry.
		oid.T_text:    {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions from TEXT to other types.
		oid.T_bit:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bool:     {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_box2d: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bytea:    {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_date: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "STRING to DATE casts depend on session DateStyle; use parse_date(string) instead",
			dateStyleAffected: true,
		},
		oid.T_float4:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_float8:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_geography: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_inet:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int2:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_interval: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility:            volatility.Immutable,
			VolatilityHint:        "STRING to INTERVAL casts depend on session IntervalStyle; use parse_interval(string) instead",
			intervalStyleAffected: true,
		},
		oid.T_jsonb:        {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_numeric:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_record:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regproc:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regprocedure: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_time: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "STRING to TIME casts depend on session DateStyle; use parse_time(string) instead",
			dateStyleAffected: true,
		},
		oid.T_timestamp: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: "STRING to TIMESTAMP casts are context-dependent because of relative timestamp strings " +
				"like 'now' and session settings such as DateStyle; use parse_timestamp(string) instead.",
			dateStyleAffected: true,
		},
		oid.T_timestamptz: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
		},
		oid.T_timetz: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "STRING to TIMETZ casts depend on session DateStyle; use parse_timetz(string) instead",
			dateStyleAffected: true,
		},
		oid.T_uuid:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varbit: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_void:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_time: {
		oid.T_interval: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_time:     {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_timetz:   {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_timestamp: {
		oid.T_date:        {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_float4:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float8:      {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int2:        {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:        {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int8:        {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric:     {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_time:        {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_timestamp:   {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_timestamptz: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: "TIMESTAMP to CHAR casts are dependent on DateStyle; consider " +
				"using to_char(timestamp) instead.",
			dateStyleAffected: true,
		},
		oid.T_char: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: `TIMESTAMP to "char" casts are dependent on DateStyle; consider ` +
				"using to_char(timestamp) instead.",
			dateStyleAffected: true,
		},
		oid.T_name: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: "TIMESTAMP to NAME casts are dependent on DateStyle; consider " +
				"using to_char(timestamp) instead.",
			dateStyleAffected: true,
		},
		oid.T_text: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: "TIMESTAMP to STRING casts are dependent on DateStyle; consider " +
				"using to_char(timestamp) instead.",
			dateStyleAffected: true,
		},
		oid.T_varchar: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility: volatility.Immutable,
			VolatilityHint: "TIMESTAMP to VARCHAR casts are dependent on DateStyle; consider " +
				"using to_char(timestamp) instead.",
			dateStyleAffected: true,
		},
	},
	oid.T_timestamptz: {
		oid.T_date:    {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Stable},
		oid.T_float4:  {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_float8:  {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int2:    {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:    {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int8:    {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_numeric: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_time:    {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Stable},
		oid.T_timestamp: {
			MaxContext:     ContextAssignment,
			origin:         contextOriginPgCast,
			Volatility:     volatility.Stable,
			VolatilityHint: "TIMESTAMPTZ to TIMESTAMP casts depend on the current timezone; consider using AT TIME ZONE 'UTC' instead",
		},
		oid.T_timestamptz: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_timetz:      {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Stable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: "TIMESTAMPTZ to CHAR casts depend on the current timezone; consider " +
				"using to_char(t AT TIME ZONE 'UTC') instead.",
		},
		oid.T_char: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: `TIMESTAMPTZ to "char" casts depend on the current timezone; consider ` +
				"using to_char(t AT TIME ZONE 'UTC') instead.",
		},
		oid.T_name: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: "TIMESTAMPTZ to NAME casts depend on the current timezone; consider " +
				"using to_char(t AT TIME ZONE 'UTC') instead.",
		},
		oid.T_text: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: "TIMESTAMPTZ to STRING casts depend on the current timezone; consider " +
				"using to_char(t AT TIME ZONE 'UTC') instead.",
		},
		oid.T_varchar: {
			MaxContext: ContextAssignment,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: "TIMESTAMPTZ to VARCHAR casts depend on the current timezone; consider " +
				"using to_char(t AT TIME ZONE 'UTC') instead.",
		},
	},
	oid.T_timetz: {
		oid.T_time:   {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_timetz: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_uuid: {
		oid.T_bytea: {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_varbit: {
		oid.T_bit:    {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_int2:   {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int4:   {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_int8:   {MaxContext: ContextExplicit, origin: contextOriginLegacyConversion, Volatility: volatility.Immutable},
		oid.T_varbit: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions to string types.
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_varchar: {
		oid.T_bpchar:   {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_char:     {MaxContext: ContextAssignment, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_name:     {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_regclass: {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Stable},
		oid.T_text:     {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		oid.T_varchar:  {MaxContext: ContextImplicit, origin: contextOriginPgCast, Volatility: volatility.Immutable},
		// Automatic I/O conversions from VARCHAR to other types.
		oid.T_bit:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bool:     {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_box2d: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_bytea:    {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_date: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "VARCHAR to DATE casts depend on session DateStyle; use parse_date(string) instead",
			dateStyleAffected: true,
		},
		oid.T_float4:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_float8:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_geography: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oidext.T_geometry:  {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_inet:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int2:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int4:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_int8:         {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_interval: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			// TODO(mgartner): This should be stable.
			Volatility:            volatility.Immutable,
			VolatilityHint:        "VARCHAR to INTERVAL casts depend on session IntervalStyle; use parse_interval(string) instead",
			intervalStyleAffected: true,
		},
		oid.T_jsonb:        {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_numeric:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_oid:          {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_record:       {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regnamespace: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regproc:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regprocedure: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regrole:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_regtype:      {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Stable},
		oid.T_time: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "VARCHAR to TIME casts depend on session DateStyle; use parse_time(string) instead",
			dateStyleAffected: true,
		},
		oid.T_timestamp: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
			VolatilityHint: "VARCHAR to TIMESTAMP casts are context-dependent because of relative timestamp strings " +
				"like 'now' and session settings such as DateStyle; use parse_timestamp(string) instead.",
			dateStyleAffected: true,
		},
		oid.T_timestamptz: {
			MaxContext: ContextExplicit,
			origin:     contextOriginAutomaticIOConversion,
			Volatility: volatility.Stable,
		},
		oid.T_timetz: {
			MaxContext:        ContextExplicit,
			origin:            contextOriginAutomaticIOConversion,
			Volatility:        volatility.Stable,
			VolatilityHint:    "VARCHAR to TIMETZ casts depend on session DateStyle; use parse_timetz(string) instead",
			dateStyleAffected: true,
		},
		oid.T_uuid:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varbit: {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_void:   {MaxContext: ContextExplicit, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
	oid.T_void: {
		oid.T_bpchar:  {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_char:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_name:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_text:    {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
		oid.T_varchar: {MaxContext: ContextAssignment, origin: contextOriginAutomaticIOConversion, Volatility: volatility.Immutable},
	},
}

// init performs sanity checks on castMap.
func init() {
	var stringTypes = [...]oid.Oid{
		oid.T_bpchar,
		oid.T_name,
		oid.T_char,
		oid.T_varchar,
		oid.T_text,
	}
	isStringType := func(o oid.Oid) bool {
		for _, strOid := range stringTypes {
			if o == strOid {
				return true
			}
		}
		return false
	}

	typeName := func(o oid.Oid) string {
		if name, ok := oidext.TypeName(o); ok {
			return name
		}
		panic(errors.AssertionFailedf("no type name for oid %d", o))
	}

	// Assert that there is a cast to and from every string type.
	for _, strType := range stringTypes {
		for otherType := range castMap {
			if strType == otherType {
				continue
			}
			strTypeName := typeName(strType)
			otherTypeName := typeName(otherType)
			if _, from := castMap[strType][otherType]; !from && otherType != oid.T_unknown {
				panic(errors.AssertionFailedf("there must be a cast from %s to %s", strTypeName, otherTypeName))
			}
			if _, to := castMap[otherType][strType]; !to {
				panic(errors.AssertionFailedf("there must be a cast from %s to %s", otherTypeName, strTypeName))
			}
		}
	}

	// Assert that each cast is valid.
	for src, tgts := range castMap {
		for tgt, ent := range tgts {
			srcStr := typeName(src)
			tgtStr := typeName(tgt)

			// Assert that MaxContext, method, and origin have been set.
			if ent.MaxContext == Context(0) {
				panic(errors.AssertionFailedf("cast from %s to %s has no MaxContext set", srcStr, tgtStr))
			}
			if ent.origin == contextOrigin(0) {
				panic(errors.AssertionFailedf("cast from %s to %s has no origin set", srcStr, tgtStr))
			}

			// Casts from a type to the same type should be implicit.
			if src == tgt {
				if ent.MaxContext != ContextImplicit {
					panic(errors.AssertionFailedf(
						"cast from %s to %s must be an implicit cast",
						srcStr, tgtStr,
					))
				}
			}

			// Automatic I/O conversions to string types are assignment casts.
			if isStringType(tgt) && ent.origin == contextOriginAutomaticIOConversion &&
				ent.MaxContext != ContextAssignment {
				panic(errors.AssertionFailedf(
					"automatic conversion from %s to %s must be an assignment cast",
					srcStr, tgtStr,
				))
			}

			// Automatic I/O conversions from string types are explicit casts.
			if isStringType(src) && !isStringType(tgt) && ent.origin == contextOriginAutomaticIOConversion &&
				ent.MaxContext != ContextExplicit {
				panic(errors.AssertionFailedf(
					"automatic conversion from %s to %s must be an explicit cast",
					srcStr, tgtStr,
				))
			}
		}
	}
}

// ForEachCast calls fn for every valid cast from a source type to a target
// type.
func ForEachCast(fn func(src, tgt oid.Oid, v volatility.Volatility)) {
	for src, tgts := range castMap {
		for tgt, c := range tgts {
			fn(src, tgt, c.Volatility)
		}
	}
}

// ValidCast returns true if a valid cast exists from src to tgt in the given
// context.
func ValidCast(src, tgt *types.T, ctx Context) bool {
	srcFamily := src.Family()
	tgtFamily := tgt.Family()

	// If src and tgt are array types, check for a valid cast between their
	// content types.
	if srcFamily == types.ArrayFamily && tgtFamily == types.ArrayFamily {
		return ValidCast(src.ArrayContents(), tgt.ArrayContents(), ctx)
	}

	// If src and tgt are tuple types, check for a valid cast between each
	// corresponding tuple element.
	//
	// Casts from a tuple type to AnyTuple are a no-op so they are always valid.
	// If tgt is AnyTuple, we continue to LookupCast below which contains a
	// special case for these casts.
	if srcFamily == types.TupleFamily && tgtFamily == types.TupleFamily && tgt != types.AnyTuple {
		srcTypes := src.TupleContents()
		tgtTypes := tgt.TupleContents()
		// The tuple types must have the same number of elements.
		if len(srcTypes) != len(tgtTypes) {
			return false
		}
		for i := range srcTypes {
			if ok := ValidCast(srcTypes[i], tgtTypes[i], ctx); !ok {
				return false
			}
		}
		return true
	}

	// If src and tgt are not both array or tuple types, check castMap for a
	// valid cast.
	c, ok := LookupCast(src, tgt, false /* intervalStyleEnabled */, false /* dateStyleEnabled */)
	if ok {
		return c.MaxContext >= ctx
	}

	return false
}

// LookupCast returns a cast that describes the cast from src to tgt if it
// exists. If it does not exist, ok=false is returned.
func LookupCast(src, tgt *types.T, intervalStyleEnabled, dateStyleEnabled bool) (Cast, bool) {
	srcFamily := src.Family()
	tgtFamily := tgt.Family()
	srcFamily.Name()

	// Unknown is the type given to an expression that statically evaluates
	// to NULL. NULL can be immutably cast to any type in any context.
	if srcFamily == types.UnknownFamily {
		return Cast{
			MaxContext: ContextImplicit,
			Volatility: volatility.Immutable,
		}, true
	}

	// Enums have dynamic OIDs, so they can't be populated in castMap. Instead,
	// we dynamically create cast structs for valid enum casts.
	if srcFamily == types.EnumFamily && tgtFamily == types.StringFamily {
		// Casts from enum types to strings are immutable and allowed in
		// assignment contexts.
		return Cast{
			MaxContext: ContextAssignment,
			Volatility: volatility.Immutable,
		}, true
	}
	if tgtFamily == types.EnumFamily {
		switch srcFamily {
		case types.StringFamily:
			// Casts from string types to enums are immutable and allowed in
			// explicit contexts.
			return Cast{
				MaxContext: ContextExplicit,
				Volatility: volatility.Immutable,
			}, true
		case types.UnknownFamily:
			// Casts from unknown to enums are immutable and allowed in implicit
			// contexts.
			return Cast{
				MaxContext: ContextImplicit,
				Volatility: volatility.Immutable,
			}, true
		}
	}

	// Casts from array types to string types are stable and allowed in
	// assignment contexts.
	if srcFamily == types.ArrayFamily && tgtFamily == types.StringFamily {
		return Cast{
			MaxContext: ContextAssignment,
			Volatility: volatility.Stable,
		}, true
	}

	// Casts from array and tuple types to string types are immutable and
	// allowed in assignment contexts.
	// TODO(mgartner): Tuple to string casts should be stable. They are
	// immutable to avoid backward incompatibility with previous versions, but
	// this is incorrect and can causes corrupt indexes, corrupt tables, and
	// incorrect query results.
	if srcFamily == types.TupleFamily && tgtFamily == types.StringFamily {
		return Cast{
			MaxContext: ContextAssignment,
			Volatility: volatility.Immutable,
		}, true
	}

	// Casts from any tuple type to AnyTuple are no-ops, so they are implicit
	// and immutable.
	if srcFamily == types.TupleFamily && tgt == types.AnyTuple {
		return Cast{
			MaxContext: ContextImplicit,
			Volatility: volatility.Immutable,
		}, true
	}

	// Casts from string types to array and tuple types are stable and allowed
	// in explicit contexts.
	if srcFamily == types.StringFamily &&
		(tgtFamily == types.ArrayFamily || tgtFamily == types.TupleFamily) {
		return Cast{
			MaxContext: ContextExplicit,
			Volatility: volatility.Stable,
		}, true
	}

	if tgts, ok := castMap[src.Oid()]; ok {
		if c, ok := tgts[tgt.Oid()]; ok {
			if intervalStyleEnabled && c.intervalStyleAffected ||
				dateStyleEnabled && c.dateStyleAffected {
				c.Volatility = volatility.Stable
			}
			return c, true
		}
	}

	// If src and tgt are the same type, the immutable cast is valid in any
	// context. This logic is intentionally after the lookup into castMap so
	// that entries in castMap are preferred.
	if src.Oid() == tgt.Oid() {
		return Cast{
			MaxContext: ContextImplicit,
			Volatility: volatility.Immutable,
		}, true
	}

	return Cast{}, false
}

// LookupCastVolatility returns the Volatility of a valid cast.
func LookupCastVolatility(
	from, to *types.T, sd *sessiondata.SessionData,
) (_ volatility.Volatility, ok bool) {
	fromFamily := from.Family()
	toFamily := to.Family()
	// Special case for casting between arrays.
	if fromFamily == types.ArrayFamily && toFamily == types.ArrayFamily {
		return LookupCastVolatility(from.ArrayContents(), to.ArrayContents(), sd)
	}
	// Special case for casting between tuples.
	if fromFamily == types.TupleFamily && toFamily == types.TupleFamily {
		fromTypes := from.TupleContents()
		toTypes := to.TupleContents()
		// Handle case where an overload makes a tuple get casted to tuple{}.
		if len(toTypes) == 1 && toTypes[0].Family() == types.AnyFamily {
			return volatility.Stable, true
		}
		if len(fromTypes) != len(toTypes) {
			return 0, false
		}
		maxVolatility := volatility.LeakProof
		for i := range fromTypes {
			v, lookupOk := LookupCastVolatility(fromTypes[i], toTypes[i], sd)
			if !lookupOk {
				return 0, false
			}
			if v > maxVolatility {
				maxVolatility = v
			}
		}
		return maxVolatility, true
	}

	intervalStyleEnabled := false
	dateStyleEnabled := false
	if sd != nil {
		intervalStyleEnabled = sd.IntervalStyleEnabled
		dateStyleEnabled = sd.DateStyleEnabled
	}

	cast, ok := LookupCast(from, to, intervalStyleEnabled, dateStyleEnabled)
	if !ok {
		return 0, false
	}
	return cast.Volatility, true
}
