// Copyright 2018 The Cockroach Authors.
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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

// This file provides facilities to support the correspondence
// between:
//
// - types.T, also called "datum types", for in-memory representations
//   via tree.Datum.
//
// - coltypes.T, also called "cast target types", which are specified
//   types in CREATE TABLE, ALTER TABLE and the CAST/:: syntax.
//
// - ColumnType, used in table descriptors.
//
// - the string representations thereof, for use in SHOW CREATE,
//   information_schema.columns and other introspection facilities.
//
// As a general rule of thumb, we are aiming for a 1-1 mapping between
// coltypes.T and ColumnType. Eventually we should even consider having
// just one implementation for both.
//
// Some notional complexity arises from the fact there are fewer
// different types.T than different coltypes.T/ColumnTypes. This is
// because some distinctions which are important at the time of data
// persistence (or casts) are not useful to keep for in-flight values;
// for example the final required precision for DECIMAL values.
//
