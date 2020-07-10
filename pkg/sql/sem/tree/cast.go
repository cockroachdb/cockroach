// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"

	"github.com/lib/pq/oid"
)

// CastContext specifies how a given Castable can be used.
// A higher value corresponds to a higher strictness.
// Higher value CastContext can use lower value CastContexts.
type CastContext uint8

const (
	_ CastContext = iota
	// CastContextImplicit implies the cast can be set implicitly
	// in any context.
	CastContextImplicit
	// CastContextAssignment implies that the cast can done implicitly
	// in assign contexts (e.g. on UPDATE and INSERT).
	CastContextAssignment
	// CastContextExplicit implies that the cast can only be used
	// in an explicit context.
	CastContextExplicit
)

// CastMethod specifies how the cast is done.
// This is legacy from postgres, but affects type overloads,
// so we also include it.
type CastMethod byte

const (
	// CastMethodFunc means that the function specified in the
	// castfunc field is used.
	CastMethodFunc = 'f'
	// CastMethodIO means that the input/output functions are used
	CastMethodIO = 'i'
	// CastMethodBinary means that the types are binary-coercible,
	// thus no conversion is required.
	CastMethodBinary = 'b'
)

// Castable defines how a method can be cast.
type Castable struct {
	Source  oid.Oid
	Target  oid.Oid
	Context CastContext
	Method  CastMethod
	// TODO(XXX): evaluate whether we should move eval.go's PerformCast
	// to here.
}

// CastableMap defines which oids can be cast to which other oids.
// The map goes from src oid -> target oid -> Castable.
// Source and Target are initialized in init().
// Adapted from `pg_cast.dat` from postgres.
// TODO(XXX): test everything in CastableMap is possible in PerformCast,
// and vice versa.
var CastableMap = map[oid.Oid]map[oid.Oid]Castable{
	oid.T_bit: {
		oid.T_bit:    {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_int4:   {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_int8:   {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_varbit: {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_bool: {
		oid.T_bpchar:  {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int4:    {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_text:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_varchar: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_box: {
		oid.T_circle:  {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_lseg:    {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_point:   {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_polygon: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_bpchar: {
		oid.T_bpchar:  {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_char:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_name:    {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_text:    {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_varchar: {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_xml:     {Method: CastMethodFunc, Context: CastContextExplicit},
	},
	oid.T_char: {
		oid.T_bpchar:  {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int4:    {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_text:    {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_varchar: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_cidr: {
		oid.T_bpchar:  {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_inet:    {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_text:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_varchar: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_circle: {
		oid.T_box:     {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_point:   {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_polygon: {Method: CastMethodFunc, Context: CastContextExplicit},
	},
	oid.T_date: {
		oid.T_timestamp:   {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_timestamptz: {Method: CastMethodFunc, Context: CastContextImplicit},
	},
	oid.T_float4: {
		oid.T_float8:  {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_int2:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int4:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int8:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_numeric: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_float8: {
		oid.T_float4:  {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int2:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int4:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int8:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_numeric: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_inet: {
		oid.T_bpchar:  {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_cidr:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_text:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_varchar: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_int2: {
		oid.T_float4:        {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_float8:        {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_int4:          {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_int8:          {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_numeric:       {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_oid:           {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regclass:      {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regconfig:     {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regdictionary: {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regnamespace:  {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regoper:       {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regoperator:   {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regproc:       {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regprocedure:  {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regrole:       {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regtype:       {Method: CastMethodFunc, Context: CastContextImplicit},
	},
	oid.T_int4: {
		oid.T_bit:           {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_bool:          {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_char:          {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_float4:        {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_float8:        {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_int2:          {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int8:          {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_money:         {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_numeric:       {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_oid:           {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regclass:      {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regconfig:     {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regdictionary: {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regnamespace:  {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regoper:       {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regoperator:   {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regproc:       {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regprocedure:  {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regrole:       {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regtype:       {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_int8: {
		oid.T_bit:           {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_float4:        {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_float8:        {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_int2:          {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int4:          {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_money:         {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_numeric:       {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_oid:           {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regclass:      {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regconfig:     {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regdictionary: {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regnamespace:  {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regoper:       {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regoperator:   {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regproc:       {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regprocedure:  {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regrole:       {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regtype:       {Method: CastMethodFunc, Context: CastContextImplicit},
	},
	oid.T_interval: {
		oid.T_interval: {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_time:     {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_json: {
		oid.T_jsonb: {Method: CastMethodIO, Context: CastContextAssignment},
	},
	oid.T_jsonb: {
		oid.T_bool:    {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_float4:  {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_float8:  {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_int2:    {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_int4:    {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_int8:    {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_json:    {Method: CastMethodIO, Context: CastContextAssignment},
		oid.T_numeric: {Method: CastMethodFunc, Context: CastContextExplicit},
	},
	oid.T_lseg: {
		oid.T_point: {Method: CastMethodFunc, Context: CastContextExplicit},
	},
	//oid.T_macaddr:{
	//	oid.T_macaddr8: {Method: CastMethodFunc, Context: CastContextImplicit},
	//},
	//oid.T_macaddr8:{
	//	oid.T_macaddr: {Method: CastMethodFunc, Context: CastContextImplicit},
	//},
	oid.T_money: {
		oid.T_numeric: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_name: {
		oid.T_bpchar:  {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_text:    {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_varchar: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_numeric: {
		oid.T_float4:  {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_float8:  {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_int2:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int4:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_int8:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_money:   {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_numeric: {Method: CastMethodFunc, Context: CastContextImplicit},
	},
	oid.T_oid: {
		oid.T_int4:          {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8:          {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_regclass:      {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regconfig:     {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regdictionary: {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regnamespace:  {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regoper:       {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regoperator:   {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regproc:       {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regprocedure:  {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regrole:       {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regtype:       {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_path: {
		oid.T_point:   {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_polygon: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	//oid.T_pg_dependencies:{
	//	oid.T_bytea: {Method: CastMethodBinary, Context: CastContextImplicit},
	//	oid.T_text:  {Method: CastMethodIO, Context: CastContextImplicit},
	//},
	//oid.T_pg_mcv_list:{
	//	oid.T_bytea: {Method: CastMethodBinary, Context: CastContextImplicit},
	//	oid.T_text:  {Method: CastMethodIO, Context: CastContextImplicit},
	//},
	//oid.T_pg_ndistinct:{
	//	oid.T_bytea: {Method: CastMethodBinary, Context: CastContextImplicit},
	//	oid.T_text:  {Method: CastMethodIO, Context: CastContextImplicit},
	//},
	oid.T_pg_node_tree: {
		oid.T_text: {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_point: {
		oid.T_box: {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_polygon: {
		oid.T_box:    {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_circle: {Method: CastMethodFunc, Context: CastContextExplicit},
		oid.T_path:   {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_point:  {Method: CastMethodFunc, Context: CastContextExplicit},
	},
	oid.T_regclass: {
		oid.T_int4: {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8: {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:  {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_regconfig: {
		oid.T_int4: {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8: {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:  {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_regdictionary: {
		oid.T_int4: {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8: {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:  {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_regnamespace: {
		oid.T_int4: {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8: {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:  {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_regoper: {
		oid.T_int4:        {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8:        {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:         {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regoperator: {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_regoperator: {
		oid.T_int4:    {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:     {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regoper: {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_regproc: {
		oid.T_int4:         {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8:         {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:          {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regprocedure: {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_regprocedure: {
		oid.T_int4:    {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8:    {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:     {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_regproc: {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_regrole: {
		oid.T_int4: {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8: {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:  {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_regtype: {
		oid.T_int4: {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_int8: {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_oid:  {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_text: {
		oid.T_bpchar:   {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_char:     {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_name:     {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regclass: {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_varchar:  {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_xml:      {Method: CastMethodFunc, Context: CastContextExplicit},
	},
	oid.T_time: {
		oid.T_interval: {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_time:     {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_timetz:   {Method: CastMethodFunc, Context: CastContextImplicit},
	},
	oid.T_timestamp: {
		oid.T_date:        {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_time:        {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_timestamp:   {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_timestamptz: {Method: CastMethodFunc, Context: CastContextImplicit},
	},
	oid.T_timestamptz: {
		oid.T_date:        {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_time:        {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_timestamp:   {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_timestamptz: {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_timetz:      {Method: CastMethodFunc, Context: CastContextAssignment},
	},
	oid.T_timetz: {
		oid.T_time:   {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_timetz: {Method: CastMethodFunc, Context: CastContextImplicit},
	},
	oid.T_varbit: {
		oid.T_bit:    {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_varbit: {Method: CastMethodFunc, Context: CastContextImplicit},
	},
	oid.T_varchar: {
		oid.T_bpchar:   {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_char:     {Method: CastMethodFunc, Context: CastContextAssignment},
		oid.T_name:     {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_regclass: {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_text:     {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_varchar:  {Method: CastMethodFunc, Context: CastContextImplicit},
		oid.T_xml:      {Method: CastMethodFunc, Context: CastContextExplicit},
	},
	oid.T_xml: {
		oid.T_bpchar:  {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_text:    {Method: CastMethodBinary, Context: CastContextAssignment},
		oid.T_varchar: {Method: CastMethodBinary, Context: CastContextAssignment},
	},
	// Here I go defining new ones!
	oid.T_uuid: {
		oid.T_bytea:   {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_text:    {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_bpchar:  {Method: CastMethodBinary, Context: CastContextImplicit},
		oid.T_varchar: {Method: CastMethodBinary, Context: CastContextImplicit},
	},
	oid.T_bytea: {
		oid.T_uuid: {Method: CastMethodBinary, Context: CastContextAssignment},
	},
}

// FindCast returns whether the given src oid can be cast to
// the target oid, given the CastContext.
// Returns a Castable object if it can be cast, and a bool whether the cast is valid.
func FindCast(src oid.Oid, tgt oid.Oid, ctx CastContext) (Castable, bool) {
	if tgts, ok := CastableMap[src]; ok {
		if castable, ok := tgts[tgt]; ok {
			return castable, ctx >= castable.Context
		}
	}
	return Castable{}, false
}

// init does sanity checks on the given CastableMap.
func init() {
	for src, tgts := range CastableMap {
		for tgt := range tgts {
			ent := CastableMap[src][tgt]
			ent.Source = src
			ent.Target = tgt
			if ent.Context == CastContext(0) {
				panic(fmt.Sprintf("cast from %d to %d has no Context set", src, tgt))
			}
			if ent.Method == CastMethod(0) {
				panic(fmt.Sprintf("cast from %d to %d has no Method set", src, tgt))
			}
			CastableMap[src][tgt] = ent
		}
	}
}
