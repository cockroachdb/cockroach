// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package plpgsqltree

// PLpgSQLVariable is a placeholder for variables. At this moment this is only a
// string representing the variable name. But potentially implementation would
// be needed to distinguish different variable types.
type PLpgSQLVariable = string
