// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tenantsettingswatcher implements an in-memory view of the
// tenant_settings table (containing overrides for tenant settings) using a
// rangefeed. This functionality is used on host cluster nodes, which allow
// tenants to retrieve the overrides and listen for changes.
package tenantsettingswatcher
