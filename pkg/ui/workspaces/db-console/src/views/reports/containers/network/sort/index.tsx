// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Checkbox, Divider } from "antd";
import React from "react";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { trackNetworkSort } from "src/util/analytics";
import { getMatchParamByName } from "src/util/query";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";

import { NetworkFilter, NetworkSort } from "..";
import { Filter } from "../filter";
import "./sort.scss";

interface ISortProps {
  onChangeFilter: (key: string, value: string) => void;
  onChangeCollapse: (checked: boolean) => void;
  deselectFilterByKey: (key: string) => void;
  collapsed: boolean;
  sort: NetworkSort[];
  filter: NetworkFilter;
}

function getSortValues(sort: NetworkSort[]) {
  return sort.map(value => ({
    value: value.id,
    label: value.id.replace(/^[a-z]/, m => m.toUpperCase()),
  }));
}

function Sort({
  onChangeFilter,
  onChangeCollapse,
  deselectFilterByKey,
  collapsed,
  sort,
  filter,
  match,
  history,
  location,
}: ISortProps & RouteComponentProps): React.ReactElement {
  const nodeId = getMatchParamByName(match, "node_id");
  const pageView = nodeId || "cluster";

  const navigateTo = (selected: DropdownOption) => {
    trackNetworkSort(selected.label);
    onChangeCollapse(false);
    location.pathname = `/reports/network/${selected.value}`;
    history.push(location);
  };

  return (
    <div className="Sort-latency">
      <Dropdown
        title="Sort By"
        options={getSortValues(sort)}
        selected={pageView}
        onChange={navigateTo}
        className="Sort-latency__dropdown--spacing"
      />
      <Filter
        sort={sort}
        onChangeFilter={onChangeFilter}
        deselectFilterByKey={deselectFilterByKey}
        filter={filter}
        dropDownClassName="Sort-latency__dropdown--spacing"
      />
      <Divider type="vertical" style={{ height: "100%" }} />
      <Checkbox
        disabled={!nodeId || nodeId === "cluster"}
        checked={collapsed}
        onChange={({ target }: any) => onChangeCollapse(target.checked)}
      >
        Collapse Nodes
      </Checkbox>
    </div>
  );
}

export default withRouter(Sort);
