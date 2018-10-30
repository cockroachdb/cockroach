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

// Package pgdate contains library functions for parsing dates
// and times in a manner that is compatible with PostgreSQL.
//
// The implementation here is inspired by the following
// https://github.com/postgres/postgres/blob/REL_10_5/src/backend/utils/adt/datetime.c
//
// The general approach is to take each input string and break it into
// contiguous runs of alphanumeric characters.  Then, we attempt to
// interpret the input in order to populate a collection of date/time
// fields.  We track which fields have been set in order to be able
// to apply various parsing heuristics to map the input chunks into
// date/time fields.
package pgdate
