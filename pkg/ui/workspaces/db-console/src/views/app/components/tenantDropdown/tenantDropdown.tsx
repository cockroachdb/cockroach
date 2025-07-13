// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { Dropdown } from "@cockroachlabs/cluster-ui";
import React from "react";

import { Button } from "src/components/button";
import { getCookieValue, setCookie } from "src/redux/cookies";
import { isSystemTenant } from "src/redux/tenants";
import { getDataFromServer } from "src/util/dataFromServer";

import ErrorBoundary from "../errorMessage/errorBoundary";

import "./tenantDropdown.styl";

const tenantIDKey = "tenant";

interface TenantDropdownState {
  currentTenant: string;
  virtualClusters: string[];
  showTenantInput: boolean;
  inputTenant: string;
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

  onSwitchTenant() {
    this.setState({
      showTenantInput: true,
      inputTenant: this.state.currentTenant || "",
    });
  }

  onInputTenantChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ inputTenant: event.target.value });
  }

  onSubmitTenant() {
    const tenant = this.state.inputTenant.trim();
    // Allow switching even if tenant name is unchanged (to refresh connection)
    // Only prevent if the input is empty and we're already on an empty/default tenant
    if (tenant || this.state.currentTenant) {
      setCookie(tenantIDKey, tenant);
      location.reload();
    }
  }

  onCancelInput() {
    this.setState({ showTenantInput: false, inputTenant: "" });
  }

  handleKeyPress(event: React.KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Enter") {
      this.onSubmitTenant();
    } else if (event.key === "Escape") {
      this.onCancelInput();
    }
  }

  constructor(props: any) {
    super(props);

    const currentTenant = getCookieValue(tenantIDKey);
    this.state = {
      currentTenant,
      virtualClusters: [],
      showTenantInput: false,
      inputTenant: "",
    };

    this.onTenantChange = this.onTenantChange.bind(this);
    this.onSwitchTenant = this.onSwitchTenant.bind(this);
    this.onInputTenantChange = this.onInputTenantChange.bind(this);
    this.onSubmitTenant = this.onSubmitTenant.bind(this);
    this.onCancelInput = this.onCancelInput.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
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
    const isInsecure = getDataFromServer().Insecure;

    // In insecure mode, show the tenant switching interface
    if (isInsecure) {
      return (
        <ErrorBoundary>
          <div className="tenant-switcher">
            {this.state.showTenantInput ? (
              <div className="tenant-input-container">
                <input
                  type="text"
                  value={this.state.inputTenant}
                  onChange={this.onInputTenantChange}
                  onKeyDown={this.handleKeyPress}
                  placeholder="Enter tenant"
                  className="tenant-input"
                  autoFocus
                  autoComplete="off"
                  autoCorrect="off"
                  autoCapitalize="off"
                  spellCheck="false"
                  name="tenantSwitcher"
                />
                <Button
                  onClick={this.onSubmitTenant}
                  type="primary"
                  className="tenant-submit"
                >
                  Apply
                </Button>
                <Button
                  onClick={this.onCancelInput}
                  type="secondary"
                  className="tenant-cancel"
                >
                  Cancel
                </Button>
              </div>
            ) : (
              <div className="tenant-display-container">
                <Button
                  onClick={this.onSwitchTenant}
                  type="secondary"
                  className="tenant-switch"
                >
                  {this.state.currentTenant
                    ? `${this.state.currentTenant} tenant`
                    : "default tenant"}
                </Button>
              </div>
            )}
          </div>
        </ErrorBoundary>
      );
    }

    // Original secure mode behavior
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
