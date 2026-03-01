// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { Dropdown } from "@cockroachlabs/cluster-ui";
import React, { useEffect, useState } from "react";

import { getCookieValue, setCookie } from "src/redux/cookies";
import { isSystemTenant } from "src/redux/tenants";
import { getDataFromServer } from "src/util/dataFromServer";

import ErrorBoundary from "../errorMessage/errorBoundary";

import "./tenantDropdown.scss";

const tenantIDKey = "tenant";

function TenantDropdown(): React.ReactElement {
  const [currentTenant] = useState(() => getCookieValue(tenantIDKey));
  const [virtualClusters, setVirtualClusters] = useState<string[]>([]);

  useEffect(() => {
    fetch("virtual_clusters", {
      method: "GET",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
    })
      .then(resp => {
        if (resp.status >= 400) {
          throw new Error(`Error response from server: ${resp.status}`);
        }
        return resp.json();
      })
      .then(respJson => {
        setVirtualClusters(respJson.virtual_clusters);
      });
  }, []);

  const createDropdownItems = () => {
    if (virtualClusters) {
      return (
        virtualClusters.map(name => {
          return { name: "Virtual cluster: " + name, value: name };
        }) || []
      );
    }
    return [];
  };

  const onTenantChange = (tenant: string) => {
    if (tenant !== currentTenant) {
      setCookie(tenantIDKey, tenant);
      location.reload();
    }
  };

  const dataFromServer = getDataFromServer();
  const isInsecure = dataFromServer.Insecure;
  // In insecure mode, show dropdown if there are >1 virtual clusters
  if (isInsecure) {
    if (virtualClusters?.length <= 1) {
      return null;
    }
  } else {
    // In secure mode, use the original logic
    if (
      !currentTenant ||
      (virtualClusters?.length < 2 && isSystemTenant(currentTenant))
    ) {
      return null;
    }
  }

  // In insecure mode, show "default" if no tenant is set
  const displayTenant = currentTenant || (isInsecure ? "default" : "");

  return (
    <ErrorBoundary>
      <Dropdown
        items={createDropdownItems()}
        onChange={(tenantID: string) => onTenantChange(tenantID)}
      >
        <div className="virtual-cluster-selected">
          {"Virtual cluster: " + displayTenant}
        </div>
      </Dropdown>
    </ErrorBoundary>
  );
}

export default TenantDropdown;
