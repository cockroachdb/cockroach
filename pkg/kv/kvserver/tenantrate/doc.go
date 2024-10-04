// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tenantrate contains logic for rate limiting client requests on a
// per-tenant basis.
//
// The package exposes a Factory which can be used to acquire a reference to a
// per-tenant Limiter. See the comment on Limiter for more details on the
// implementation and behavior.
package tenantrate
