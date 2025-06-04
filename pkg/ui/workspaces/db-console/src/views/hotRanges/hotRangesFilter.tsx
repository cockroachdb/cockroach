// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { WarningOutlined } from "@ant-design/icons";
import {
  FilterDropdown,
  FilterSearchOption,
  FilterCheckboxOption,
  FilterCheckboxOptionItem,
  NodeSelector,
  NodeID,
} from "@cockroachlabs/cluster-ui";
import { Row, Col } from "antd";
import classNames from "classnames/bind";
import React, { useMemo } from "react";
import { useSelector } from "react-redux";

import { selectDatabases } from "../statements/statementsPage";

import styles from "./hotRanges.module.styl";
import { Filters } from "./useFilters";

const cx = classNames.bind(styles);

const noop = () => {};

export type HotRangesFilterProps = {
  filters: Filters;
  applyFilters: (filters: Filters) => void;
};

// HotRangesFilter is the component within the hot ranges page
// which contains all of the filter controls. The definition of the
// filter state and filter management utilites can be found in
// useFilter.ts.
export const HotRangesFilter = (props: HotRangesFilterProps) => {
  const { filters, applyFilters } = props;
  const [localFilters, setLocalFilters] = React.useState<Filters>(filters);
  const { store_id, databases, table, index } = localFilters;
  const { nodeIds } = filters;

  // The below functions are all setters for modifying the local filters.
  const mergeFilters = (extraFilters: Filters) =>
    setLocalFilters({ ...localFilters, ...extraFilters });

  // The below setters are used for the various filters.
  const setStore = (store_id: string) => mergeFilters({ store_id });
  const setDatabases = (databaseOptions: FilterCheckboxOptionItem[]) => {
    const databases = databaseOptions.map(
      (o: FilterCheckboxOptionItem) => o.value,
    );
    mergeFilters({ databases });
  };
  const setTable = (table: string) => mergeFilters({ table });
  const setIndex = (index: string) => mergeFilters({ index });

  // Setup the options and selected values for the database filter.
  const databaseNames = useSelector(selectDatabases);
  const databaseOptions = databaseNames.map((db: string) => ({
    label: db,
    value: db,
  }));

  const selectedOptions = (values: string[]) =>
    values.map((v: string) => ({ label: v, value: v }));

  // These variables are used for rendering the expandable filters button.
  const filtersDisabled = nodeIds.length === 0;
  const filterCount = useMemo(() => {
    const allFilters = [store_id, table, index, ...databases];
    return allFilters.filter(f => !!f).length;
  }, [store_id, table, index, databases]);

  // Submission hooks for both the grouped filters as well as
  // the node selector.
  const onSubmit = () => {
    applyFilters({ ...localFilters, nodeIds: nodeIds });
  };

  // The only difference on this hook is that it allows the caller
  // to pass in the nodeIds.
  const onNodeIdApply = (nodeIds: NodeID[]) => {
    applyFilters({ ...localFilters, nodeIds });
  };

  return (
    <Row className={cx("hotranges-dropdown-filter")}>
      <Col className={cx("hotranges-node-selector")}>
        <NodeSelector
          initialValue={nodeIds}
          onChange={onNodeIdApply}
          allWarning={
            <div className={cx("warning-all-nodes")}>
              <WarningOutlined className={cx("warning-icon")} />
              <span className={cx("warning-all-text")}>
                Might take a longer time to load
              </span>
            </div>
          }
        />
      </Col>
      <Col className={filtersDisabled && cx("filters-disabled")}>
        <FilterDropdown label={`Filter (${filterCount})`} onSubmit={onSubmit}>
          <FilterCheckboxOption
            label="Databases"
            options={databaseOptions}
            placeholder="Select"
            value={selectedOptions(databases)}
            onSelectionChanged={setDatabases}
          />
          <FilterSearchOption
            label="Table"
            onChanged={setTable}
            value={table}
            onSubmit={noop}
          />
          <FilterSearchOption
            label="Index"
            onChanged={setIndex}
            value={index}
            onSubmit={noop}
          />
          <FilterSearchOption
            label="Store ID"
            onChanged={setStore}
            value={store_id}
            onSubmit={noop}
          />
        </FilterDropdown>
      </Col>
    </Row>
  );
};
