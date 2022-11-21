// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import {
  selectCurrentTenantFromCookies,
  selectTenantsFromCookies,
} from "src/redux/tenantOptions";
import React from "react";
import { Dropdown } from "src/components/dropdown";
import ErrorBoundary from "../errorMessage/errorBoundary";
import { CaretDown } from "@cockroachlabs/icons";
import "./tenantDropdown.styl";

const tenantIDKey = "tenant";

const TenantDropdown = () => {
  const tenants = selectTenantsFromCookies();
  const currentTenant = selectCurrentTenantFromCookies();

  const createDropdownItems = () => {
    return (
      tenants?.map(tenantID => {
        return { name: "Tenant " + tenantID, value: tenantID };
      }) || []
    );
  };

  const setTenantCookie = (tenant: string) => {
    if (tenant !== currentTenant) {
      document.cookie = `${tenantIDKey}=${tenant};path=/`;
      location.reload();
    }
  };

  if (tenants.length == 0) {
    return null;
  }

  return (
    <ErrorBoundary>
      <Dropdown
        className="tenant-dropdown"
        items={createDropdownItems()}
        onChange={tenantID => setTenantCookie(tenantID)}
        icon={<CaretDown className="tenant-caret-down" />}
      >
        <div className="tenant-selected">{"Tenant " + currentTenant}</div>
      </Dropdown>
    </ErrorBoundary>
  );
};

export default TenantDropdown;
