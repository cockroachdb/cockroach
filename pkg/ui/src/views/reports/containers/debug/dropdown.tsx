// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Icon } from "antd";
import _ from "lodash";
import { NodesSummary, nodesSummarySelector } from "oss/src/redux/nodes";
import { AdminUIState } from "oss/src/redux/state";
import React from "react";
import { connect } from "react-redux";
import { withRouter } from "react-router";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";

interface INodesDropdownProps {
  nodesSummary: NodesSummary;
}

interface INodesDropdownState {
  location: string;
  focused: boolean;
}

class NodesDropdown extends React.PureComponent<INodesDropdownProps, INodesDropdownState> {
  state = {
    location: "",
    focused: false,
  };

  findSelectedValue = () => {
    const value = _.find(this.getOptionsForSelect(), (app) => this.getHostname(app.value) === window.location.hostname);
    if (!value) {
      return {
        value: window.location.hostname,
        label: window.location.hostname,
      };
    }
    return value;
  }

  getOptionsForSelect = () => this.props.nodesSummary.nodeIDs.map((value: string) => ({
    value: this.props.nodesSummary.nodeDisplayNameByID[value],
    label: `Node ${value}`,
  }))

  getHostname = (value: string) => value.split(" ")[0];

  openApp = (app: DropdownOption) => () => window.open(`http://${this.getHostname(app.value)}/#/debug/`, "_self");

  toggleDropDown = () => this.setState({ focused: !this.state.focused });

  arrowRenderer = (isOpen: boolean) => {
    if (isOpen) {
      return <span><Icon type="caret-up" /></span>;
    }
    return <span className="active"><Icon type="caret-down" /></span>;
  }

  renderOptions = () => {
    const selected = this.findSelectedValue();
    return this.getOptionsForSelect().map((option: DropdownOption) => (
      <a onClick={this.openApp(option)} className={`option ${selected.value === option.value ? "selected" : ""}`}>{option.label}</a>
    ));
  }

  renderContent = () => {
    const { focused } = this.state;
    return (
      <div className="dropdown-list">
        <div className="click-zone" onClick={this.toggleDropDown}/>
        {focused && <div className="trigger-container" onClick={this.toggleDropDown} />}
        <div className="trigger-wrapper">
          <div
            className={`trigger Select ${focused ? "is-open" : ""}`}
          >
            <span className="Select-value-label">
              {this.findSelectedValue().label}
            </span>
            <div className="Select-control">
              <div className="Select-arrow-zone">
                {this.arrowRenderer(focused)}
              </div>
            </div>
          </div>
          {focused && (
            <div className={`dropdown-options`}>
              {this.renderOptions()}
            </div>
          )}
        </div>
      </div>
    );
  }

  render() {
    return (
      <Dropdown
        title="Web server"
        selected={this.findSelectedValue().value}
        palette="blue"
        focused={this.state.focused}
        content={this.renderContent()}
      />
    );
  }
}

export default withRouter(connect(
  (state: AdminUIState) => ({
    nodesSummary: nodesSummarySelector(state),
  }),
)(NodesDropdown as any));
