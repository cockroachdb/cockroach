// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Checkbox, Divider } from "antd";
import Dropdown, { DropdownOption } from "oss/src/views/shared/components/dropdown";
import PropTypes from "prop-types";
import React from "react";
import { InjectedRouter, RouterState } from "react-router";
import { NetworkFilter, NetworkSort } from "..";
import { Filter } from "../filter";
import "./sort.styl";

interface ISortProps {
  onChangeFilter: (key: string, value: string) => void;
  onChangeCollapse: (checked: boolean) => void;
  deselectFilterByKey: (key: string) => void;
  collapsed: boolean;
  sort: NetworkSort[];
  filter: NetworkFilter;
}

export class Sort extends React.Component<ISortProps, {}> {
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState; };

  onChange = ({ target }: any) => this.props.onChangeCollapse(target.checked);

  pageView = () => {
    const { router } = this.context;
    return router.params.node_id || "cluster";
  }

  navigateTo = (selected: DropdownOption) => {
    this.props.onChangeCollapse(false);
    this.context.router.push(`reports/network/${selected.value}`);
  }

  componentDidMount() {
    this.setDefaultSortValue("region");
  }

  setDefaultSortValue = (sortValue: string) => {
    const isDefaultValuePresent = this.getSortValues(this.props.sort).find(e => e.value === sortValue);
    if (isDefaultValuePresent) {
      this.navigateTo(isDefaultValuePresent);
    }
  }

  getSortValues = (sort: NetworkSort[]) => sort.map(value => {
    return { value: value.id, label: value.id.replace(/^[a-z]/, m => m.toUpperCase()) };
  })

  render() {
    const { router } = this.context;
    const { collapsed, sort, onChangeFilter, deselectFilterByKey, filter } = this.props;
    return (
      <div className="Sort-latency">
        <Dropdown
          title="Sort By"
          options={this.getSortValues(sort)}
          selected={this.pageView()}
          onChange={this.navigateTo}
        />
        <Filter sort={sort} onChangeFilter={onChangeFilter} deselectFilterByKey={deselectFilterByKey} filter={filter} />
        <Divider type="vertical" style={{ height: "100%" }} />
        <Checkbox disabled={!router.params.node_id || router.params.node_id === "cluster"} checked={collapsed} onChange={this.onChange}>Collapse Nodes</Checkbox>
      </div>
    );
  }
}
