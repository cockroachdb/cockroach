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
  getCookieValue,
  selectTenantsFromMultitenantSessionCookie,
  setCookie,
} from "src/redux/cookies";
import React from "react";
import { Dropdown } from "@cockroachlabs/cluster-ui";
import ErrorBoundary from "../errorMessage/errorBoundary";
import "./tenantDropdown.styl";

const tenantIDKey = "tenant";

const TenantDropdown = () => {
  const tenants = selectTenantsFromMultitenantSessionCookie();
  const currentTenant = getCookieValue(tenantIDKey);

  const createDropdownItems = () => {
    return (
      tenants?.map(tenantID => {
        return { name: "Tenant " + tenantID, value: tenantID };
      }) || []
    );
  };

  const onTenantChange = (tenant: string) => {
    if (tenant !== currentTenant) {
      setCookie(tenantIDKey, tenant);
      location.reload();
    }
  };

  if (tenants.length == 0) {
    return null;
  }

  return (
    <ErrorBoundary>
      <Dropdown
        items={createDropdownItems()}
        onChange={tenantID => onTenantChange(tenantID)}
      >
        <div className="tenant-selected">{"Tenant " + currentTenant}</div>
      </Dropdown>
    </ErrorBoundary>
  );
};

export default TenantDropdown;
