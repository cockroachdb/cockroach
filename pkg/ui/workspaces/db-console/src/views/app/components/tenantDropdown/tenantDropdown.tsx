// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { Dropdown } from "@cockroachlabs/cluster-ui";
import React from "react";

import { getCookieValue, setCookie } from "src/redux/cookies";
import { isSystemTenant } from "src/redux/tenants";

import ErrorBoundary from "../errorMessage/errorBoundary";

import "./tenantDropdown.styl";

const tenantIDKey = "tenant";

interface TenantDropdownState {
  currentTenant: string;
  virtualClusters: string[];
}

export default class TenantDropdown extends React.Component<
  {},
  TenantDropdownState
> {
  createDropdownItems() {
    if (this.state.virtualClusters) {
      return (
        this.state.virtualClusters.map(name => {
          return { name: "Virtual cluster: " + name, value: name };
        }) || []
      );
    } else {
      return [];
    }
  }

  onTenantChange(tenant: string) {
    if (tenant !== this.state.currentTenant) {
      setCookie(tenantIDKey, tenant);
      location.reload();
    }
  }

  constructor(props: any) {
    super(props);

    const currentTenant = getCookieValue(tenantIDKey);
    this.state = {
      currentTenant,
      virtualClusters: [],
    };

    this.onTenantChange = this.onTenantChange.bind(this);
  }

  componentDidMount() {
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
        this.setState({
          virtualClusters: respJson.virtual_clusters,
        });
      });
  }

  render() {
    if (
      !this.state.currentTenant ||
      (this.state.virtualClusters?.length < 2 &&
        isSystemTenant(this.state.currentTenant))
    ) {
      return null;
    }

    return (
      <ErrorBoundary>
        <Dropdown
          items={this.createDropdownItems()}
          onChange={(tenantID: string) => this.onTenantChange(tenantID)}
        >
          <div className="virtual-cluster-selected">
            {"Virtual cluster: " + this.state.currentTenant}
          </div>
        </Dropdown>
      </ErrorBoundary>
    );
  }
}
