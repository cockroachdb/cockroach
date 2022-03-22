// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package clierrorplus contains facilities that would nominally belong
// to package `clierror` instead, but which we do not wish to place
// there to prevent `clierror` from depending to more complex packages.
// We want `clierror` to remain lightweight so that the `cockroach-sql`
// standalone shell remains small.
package clierrorplus
