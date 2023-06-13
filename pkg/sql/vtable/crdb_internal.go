// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vtable

// CrdbInternalBuiltinFunctionComments describes the schema of the
// crdb_internal.kv_builtin_function_comments table.
var CrdbInternalBuiltinFunctionComments = `
CREATE TABLE crdb_internal.kv_builtin_function_comments (
  oid         OID NOT NULL,
  description STRING NOT NULL
)`

// CrdbInternalCatalogComments describes the schema of the
// crdb_internal.kv_catalog_comments table.
var CrdbInternalCatalogComments = `
CREATE TABLE crdb_internal.kv_catalog_comments (
  classoid    OID NOT NULL,
  objoid      OID NOT NULL,
  objsubid    INT4 NOT NULL,
  description STRING NOT NULL
)`
