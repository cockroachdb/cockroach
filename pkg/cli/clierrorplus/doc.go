// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package clierrorplus contains facilities that would nominally belong
// to package `clierror` instead, but which we do not wish to place
// there to prevent `clierror` from depending to more complex packages.
// We want `clierror` to remain lightweight so that the `cockroach-sql`
// standalone shell remains small.
package clierrorplus
