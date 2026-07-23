// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package clienturl provides glue between:
//
//   - security/clientconnurl, which is able to translate configuration
//     parameters for SQL clients and compute security parameters,
//
// - cli/clisqlcfg, which serves as input for a SQL shell.
//
// It also provides the URL parser which is able to interleave
// itself in-between discrete flags.
//
// This logic is placed in a package separate from
// security/clientsecopts to avoid a dependency from
// security/clientsecopts into spf13/cobra and spf13/pflag.
package clienturl
