// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import cookie from "cookie";
import {
  selectCurrentTenantIDFromCookies,
  selectTenantsFromCookies,
} from "src/redux/tenantOptions";
import React from "react";
import { Dropdown } from "src/components/dropdown";
import ErrorBoundary from "../errorMessage/errorBoundary";
import { CaretDown } from "@cockroachlabs/icons";
import "./tenantDropdown.styl";

const tenantIDKey = "tenant_id";

const TenantDropdown = () => {
  const tenants = selectTenantsFromCookies();
  const currentTenantID = selectCurrentTenantIDFromCookies();

  const createDropdownItems = () => {
    return (
      tenants?.map(tenantID => {
        return { name: "Tenant " + tenantID, value: tenantID };
      }) || []
    );
  };

  const setTenantIDCookie = (tenantID: string) => {
    document.cookie = `${tenantIDKey}=${tenantID};path=/`;
    location.reload();
  };
  console.log(cookie.parse(document.cookie));
  if (tenants.length == 0) {
    return null;
  }

  return (
    <ErrorBoundary>
      <Dropdown
        className="tenant-dropdown"
        items={createDropdownItems()}
        onChange={tenantID => setTenantIDCookie(tenantID)}
        icon={<CaretDown className="tenant-caret-down" />}
      >
        <div className="tenant-selected">{"Tenant " + currentTenantID}</div>
      </Dropdown>
    </ErrorBoundary>
  );
};

export default TenantDropdown;
