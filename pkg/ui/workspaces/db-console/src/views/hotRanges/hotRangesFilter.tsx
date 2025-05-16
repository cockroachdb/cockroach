// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
import classNames from "classnames/bind";
import React, { useMemo } from "react"
import { useSelector } from "react-redux";
import { Row, Col } from "antd";

import {
  FilterDropdown,
  FilterSearchOption,
  FilterCheckboxOption,
  FilterCheckboxOptionItem,
} from "@cockroachlabs/cluster-ui";

import styles from "./hotRanges.module.styl";

import { selectNodeLocalities } from "src/redux/localities";
import { Filters } from "./useFilters";
import NodeIDSelector from "./nodeIdSelect";
import { selectDatabases } from "../statements/statementsPage";

const cx = classNames.bind(styles);

export type HotRangesFilterProps = {
  filters: Filters;
  applyFilters: (filters: Filters) => void;
};

export const HotRangesFilter = (props: HotRangesFilterProps) => {
  const { filters, applyFilters } = props;
  const [localFilters, setLocalFilters] = React.useState<Filters>(filters);
  const { node_ids, store_id, databases, table, index, localities } = localFilters;
  const mergeFilters = (extraFilters: Filters) => setLocalFilters({ ...localFilters, ...extraFilters });

  // The below setters are used for the various filters.
  const setStore = (store_id: string) => mergeFilters({ store_id });
  const setDatabases = (databaseOptions: FilterCheckboxOptionItem[]) => {
    const databases = databaseOptions.map(o => o.value);
    mergeFilters({ databases });
  }
  const setTable = (table: string) => mergeFilters({ table });
  const setIndex = (index: string) => mergeFilters({ index });
  const setLocalities = (localityOptions: FilterCheckboxOptionItem[]) => {
    const localities = localityOptions.map(o => o.value);
    mergeFilters({ localities });
  }

  const nodeIdToLocalityMap = useSelector(selectNodeLocalities);
  const localityOptions = Array.from(nodeIdToLocalityMap.values())
    .sort()
    .map(locality => ({ label: locality, value: locality }));

  const onSubmit = () => {
    applyFilters(localFilters);
  };

  const onNodeIdSubmit = (node_ids: string[]) => {
    applyFilters({ ...localFilters, node_ids });
  }

  const databaseNames = useSelector(selectDatabases)
  const databaseOptions = databaseNames.map(db => ({ label: db, value: db }));

  const selectedOptions = (values: string[]) => values.map((v: string) => ({ label: v, value: v }));

  const filterCount = useMemo(() => {
    const allFilters = [store_id, table, index, ...localities, ...databases];
    return allFilters.filter(f => !!f).length;
  }, [filters]);

  return (
    <Row className={cx("hotranges-dropdown-filter")}>
      <Col className={cx("hotranges-node-selector")}>
        <NodeIDSelector
          node_ids={node_ids}
          onSubmit={onNodeIdSubmit}
        />
      </Col>
      <Col>
        <FilterDropdown
          label={`Filter (${filterCount})`}
          onSubmit={onSubmit}
        >
          <FilterSearchOption
            label="Store ID"
            onChanged={setStore}
            value={store_id}
          />
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
          />
          <FilterSearchOption
            label="Index"
            onChanged={setIndex}
            value={index}
          />
          <FilterCheckboxOption
            label="Locality"
            options={localityOptions}
            placeholder="Select"
            value={selectedOptions(localities)}
            onSelectionChanged={setLocalities}
          />

        </FilterDropdown>
      </Col>
    </Row >
  );
};

