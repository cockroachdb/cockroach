// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// isSystemTenant checks whether the provided tenant name is the
// system tenant.
import { SYSTEM_TENANT_NAME } from "./cookies";

export const isSystemTenant = (tenantName: string): boolean => {
  return tenantName === SYSTEM_TENANT_NAME;
};
