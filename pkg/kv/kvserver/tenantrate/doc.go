// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package tenantrate contains logic for rate limiting client requests on a
// per-tenant basis.
//
// The package exposes a Factory which can be used to acquire a reference to a
// per-tenant Limiter. See the comment on Limiter for more details on the
// implementation and behavior.
package tenantrate
