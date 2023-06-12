// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { createSelector } from "reselect";
import { DropdownOption } from "../views/shared/components/dropdown";
import { SYSTEM_TENANT_NAME } from "./cookies";
import { AdminUIState } from "./state";

export const tenantsSelector = (state: AdminUIState) =>
  state.cachedData.tenants?.data?.tenants;

// tenantDropdownOptions makes an array of dropdown options from
// the tenants found in the redux state. It also adds a synthetic
// all option which aggregates all metrics.
export const tenantDropdownOptions = createSelector(
  tenantsSelector,
  tenantsList => {
    const tenantOptions: DropdownOption[] = [{ label: "All", value: "" }];
    tenantsList?.map(tenant =>
      tenantOptions.push({
        label: tenant.tenant_name,
        value: tenant.tenant_id?.id?.toString(),
      }),
    );
    return tenantOptions;
  },
);

// isSystemTenant checks whether the provided tenant name is the
// system tenant.
export const isSystemTenant = (tenantName: string): boolean => {
  return tenantName === SYSTEM_TENANT_NAME;
};
