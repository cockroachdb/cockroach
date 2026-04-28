// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { DropdownOption } from "../views/shared/components/dropdown";

import { SYSTEM_TENANT_NAME } from "./cookies";

// isSystemTenant checks whether the provided tenant name is the
// system tenant.
export const isSystemTenant = (tenantName: string): boolean => {
  return tenantName === SYSTEM_TENANT_NAME;
};

export const containsApplicationTenants = (
  tenantOptions: DropdownOption[],
): boolean =>
  tenantOptions.some(
    t =>
      t.label !== SYSTEM_TENANT_NAME &&
      t.label !== "All",
  );

// isSecondaryTenant checks whether the provided tenant is secondary or not.
// null or empty values are considered false since (for the current main use case)
// we do not want to display the empty tenant graph state if a graph doesn't
// provide the tenantSource prop.
export const isSecondaryTenant = (tenant: string | undefined): boolean => {
  if (!tenant || tenant === "" || tenant === SYSTEM_TENANT_NAME) {
    return false;
  }
  return true;
};
