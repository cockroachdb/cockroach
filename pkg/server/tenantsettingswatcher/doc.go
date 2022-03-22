// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package tenantsettingswatcher implements an in-memory view of the
// tenant_settings table (containing overrides for tenant settings) using a
// rangefeed. This functionality is used on host cluster nodes, which allow
// tenants to retrieve the overrides and listen for changes.
package tenantsettingswatcher
