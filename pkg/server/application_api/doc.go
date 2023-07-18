// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package application_api pertains to the RPC and HTTP APIs exposed by
// the application layers, including SQL and tenant-scoped HTTP.
// Storage-level APIs (e.g. KV node inspection) are in the
// storage_api package.
package application_api
