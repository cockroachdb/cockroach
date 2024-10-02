// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
