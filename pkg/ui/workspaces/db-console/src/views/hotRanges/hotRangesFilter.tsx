// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Button,
  FilterCheckboxOption,
  FilterCheckboxOptionItem,
  FilterCheckboxOptionsType,
  FilterDropdown,
  FilterSearchOption,
} from "@cockroachlabs/cluster-ui";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";
import isArray from "lodash/isArray";
import isEmpty from "lodash/isEmpty";
import isString from "lodash/isString";
import noop from "lodash/noop";
import React, { useCallback, useEffect, useState, useMemo } from "react";
import ReactDOM from "react-dom";
import { useHistory } from "react-router-dom";

import styles from "./hotRanges.module.styl";

type HotRange = cockroach.server.serverpb.HotRangesResponseV2.IHotRange;

const cx = classNames.bind(styles);

export type HotRangesFilterProps = {
  hotRanges: HotRange[];
  onChange: (filteredHotRanges: HotRange[]) => void;
  nodeIdToLocalityMap: Map<number, string>;
  clearButtonContainer: Element;
};

export const HotRangesFilter = (props: HotRangesFilterProps) => {
  const { hotRanges, onChange, nodeIdToLocalityMap } = props;

  // nodeIdsOptions is a list of uniq node IDs that are available in
  // provided list of hot ranges.
  const nodeIdsOptions = useMemo(
    () =>
      Array.from(new Set(hotRanges.map(r => `${r.node_id}`)))
        .sort()
        .map(nodeId => ({ label: nodeId, value: nodeId })),
    [hotRanges],
  );

  // databaseOptions is a list of uniq database names that are available in
  // provided list of hot ranges.
  const databaseOptions = useMemo(
    () =>
      Array.from(new Set(hotRanges.map(r => r.databases).flat()))
        .filter(i => !isEmpty(i))
        .sort()
        .map(dbName => ({ label: dbName, value: dbName })),
    [hotRanges],
  );

  // localitiesOptions is a list of uniq localities that are available in
  // provided list of hot ranges.
  const localitiesOptions = useMemo(
    () =>
      Array.from(nodeIdToLocalityMap.values())
        .sort()
        .map(locality => ({ label: locality, value: locality })),
    [nodeIdToLocalityMap],
  );

  const history = useHistory();

  // define filter names that are used in URL search params to preserve filter options.
  const nodeIds = "node_ids";
  const storeId = "store_id";
  const dbNames = "db_names";
  const table = "table";
  const indexName = "index";
  const localities = "localities";

  // initialize states of filter options with values from URL search params.
  const [filterNodeIds, setFilterNodeIds] = useState<FilterCheckboxOptionsType>(
    [],
  );
  const [filterStoreId, setFilterStoreId] = useState<string>();
  const [filterDbNames, setFilterDbNames] = useState<FilterCheckboxOptionsType>(
    [],
  );
  const [filterTableName, setFilterTableName] = useState<string>();
  const [filterIndexName, setFilterIndexName] = useState<string>();
  const [filterLocalities, setFilterLocalities] =
    useState<FilterCheckboxOptionsType>([]);

  // stagedFilteredHotRanges preserves intermediate filters states that are selected but
  // is not Applied yet.
  const [stagedFilteredHotRanges, setStagedFilteredHotRanges] = useState<
    HotRange[]
  >([]);

  // getFiltersCount returns how many filter options are selected.
  const getFiltersCount = useCallback(
    (filters: Array<FilterCheckboxOptionsType | string>) =>
      filters.filter(f => !isEmpty(f)).length,
    [],
  );

  const [filtersCount, setFiltersCount] = useState<number>(0);

  // filterBy function performs actual filtering of hot ranges regarding provided
  // filters options.
  const filterBy = useCallback(
    (
      ranges: HotRange[],
      nodeIds: FilterCheckboxOptionsType,
      storeId: string,
      dbNames: FilterCheckboxOptionsType,
      tableName: string,
      indexName: string,
      localities: FilterCheckboxOptionsType,
    ) => {
      const hasNoFilters = [
        nodeIds,
        storeId,
        dbNames,
        tableName,
        indexName,
        localities,
      ].every(isEmpty);

      if (hasNoFilters) {
        return ranges;
      }

      let filtered = [...ranges];

      if (!isEmpty(nodeIds)) {
        filtered = filtered.filter(r =>
          nodeIds.some(
            (f: FilterCheckboxOptionItem) => f.value === r.node_id?.toString(),
          ),
        );
      }

      if (!isEmpty(storeId)) {
        filtered = filtered.filter(r =>
          r.store_id.toString().includes(storeId),
        );
      }

      if (!isEmpty(dbNames)) {
        filtered = filtered.filter(r =>
          dbNames.some((f: FilterCheckboxOptionItem) =>
            r.databases?.includes(f.value),
          ),
        );
      }

      if (!isEmpty(tableName)) {
        filtered = filtered.filter(r => r.tables?.includes(tableName));
      }

      if (!isEmpty(indexName)) {
        filtered = filtered.filter(r => r.indexes?.includes(indexName));
      }

      if (!isEmpty(localities)) {
        filtered = filtered.filter(r => {
          const locality = nodeIdToLocalityMap.get(r.node_id);
          return localities.some(
            (f: FilterCheckboxOptionItem) => f.value === locality,
          );
        });
      }
      return filtered;
    },
    [nodeIdToLocalityMap],
  );

  // Following effect is required to filter hot ranges every time when
  // hot ranges list is updated after filters are changed (it is possible
  // when: a) filter options are retrieved from URL search params; b) when
  // hot ranges are periodically updated and new data is received from server).
  useEffect(() => {
    const searchParams = new URLSearchParams(history.location.search);
    const nodeIdsFilter = (searchParams.get(nodeIds) || "")
      .split("|")
      .filter(i => !isEmpty(i))
      .map(i => ({ label: i, value: i }));
    const storeIdFilter = searchParams.get(storeId);
    const dbNamesFilter = (searchParams.get(dbNames) || "")
      .split("|")
      .filter(i => !isEmpty(i))
      .map(i => ({ label: i, value: i }));
    const tableNameFilter = searchParams.get(table);
    const indexNameFilter = searchParams.get(indexName);
    const localitiesFilter = (searchParams.get(localities) || "")
      .split("|")
      .filter(i => !isEmpty(i))
      .map(i => ({ label: i, value: i }));

    setFilterNodeIds(nodeIdsFilter);
    setFilterStoreId(storeIdFilter);
    setFilterDbNames(dbNamesFilter);
    setFilterTableName(tableNameFilter);
    setFilterIndexName(indexNameFilter);
    setFilterLocalities(localitiesFilter);

    setFiltersCount(
      getFiltersCount([
        nodeIdsFilter,
        storeIdFilter,
        dbNamesFilter,
        tableNameFilter,
        indexNameFilter,
        localitiesFilter,
      ]),
    );

    const filtered = filterBy(
      hotRanges,
      nodeIdsFilter,
      storeIdFilter,
      dbNamesFilter,
      tableNameFilter,
      indexNameFilter,
      localitiesFilter,
    );
    onChange(filtered);
  }, [hotRanges, onChange, history, filterBy, getFiltersCount]);

  useEffect(() => {
    const filtered = filterBy(
      hotRanges,
      filterNodeIds,
      filterStoreId,
      filterDbNames,
      filterTableName,
      filterIndexName,
      filterLocalities,
    );
    setStagedFilteredHotRanges(filtered);
  }, [
    filterNodeIds,
    filterStoreId,
    filterDbNames,
    filterTableName,
    filterIndexName,
    filterLocalities,
    hotRanges,
    filterBy,
  ]);

  const getSearchParams = useCallback(() => {
    const params = new URLSearchParams();
    const filters: Array<[string, FilterCheckboxOptionsType | string]> = [
      [nodeIds, filterNodeIds],
      [storeId, filterStoreId],
      [dbNames, filterDbNames],
      [table, filterTableName],
      [indexName, filterIndexName],
      [localities, filterLocalities],
    ];
    filters
      .filter(f => !isEmpty(f[1]))
      .forEach(([name, value]) => {
        if (isArray(value)) {
          params.set(name, value.map(f => f.value).join("|"));
        }
        if (isString(value)) {
          params.set(name, value);
        }
      });
    return params;
  }, [
    filterNodeIds,
    filterStoreId,
    filterDbNames,
    filterTableName,
    filterIndexName,
    filterLocalities,
  ]);

  // onApply is a callback function that is fired when user click "Apply" button
  // in DropdownFilter.
  const onApply = useCallback(() => {
    setFiltersCount(
      getFiltersCount([
        filterNodeIds,
        filterStoreId,
        filterDbNames,
        filterTableName,
        filterIndexName,
        filterLocalities,
      ]),
    );
    onChange(stagedFilteredHotRanges);
    history.location.search = getSearchParams().toString();
    history.replace(history.location);
  }, [
    getFiltersCount,
    filterNodeIds,
    filterStoreId,
    filterDbNames,
    filterTableName,
    filterIndexName,
    filterLocalities,
    onChange,
    stagedFilteredHotRanges,
    history,
    getSearchParams,
  ]);

  // onClearClick function clears all filters, URL search params, and resets filters counter.
  const onClearClick = useCallback(() => {
    setFilterNodeIds([]);
    setFilterStoreId(undefined);
    setFilterDbNames([]);
    setFilterTableName(undefined);
    setFilterIndexName(undefined);
    setFilterLocalities([]);
    onChange(hotRanges);
    setFiltersCount(0);
    history.location.search = getSearchParams().toString();
    history.replace(history.location);
  }, [onChange, hotRanges, history, getSearchParams]);

  return (
    <div>
      <FilterDropdown
        label={`Filter (${filtersCount})`}
        onSubmit={onApply}
        className={cx("hotranges-dropdown-filter")}
      >
        <FilterCheckboxOption
          label="Node ID"
          options={nodeIdsOptions}
          placeholder="Select"
          value={filterNodeIds}
          onSelectionChanged={(options: any) => {
            setFilterNodeIds(options);
          }}
        />
        <FilterSearchOption
          label="Store ID"
          onChanged={setFilterStoreId}
          onSubmit={noop}
          value={filterStoreId}
        />
        <FilterCheckboxOption
          label="Database"
          options={databaseOptions}
          value={filterDbNames}
          placeholder="Select"
          onSelectionChanged={(options: any) => {
            setFilterDbNames(options as any);
          }}
        />
        <FilterSearchOption
          label="Table"
          onChanged={setFilterTableName}
          onSubmit={noop}
          value={filterTableName}
        />
        <FilterSearchOption
          label="Index"
          onChanged={setFilterIndexName}
          onSubmit={noop}
          value={filterIndexName}
        />
        <FilterCheckboxOption
          label="Locality"
          options={localitiesOptions}
          placeholder="Select"
          onSelectionChanged={(o: any) => setFilterLocalities(o as any)}
          value={filterLocalities}
        />
      </FilterDropdown>
      {props.clearButtonContainer &&
        filtersCount > 0 &&
        ReactDOM.createPortal(
          <>
            <span>&nbsp;&nbsp;&nbsp;-</span>
            <Button onClick={onClearClick} type="flat" size="small">
              clear filter
            </Button>
          </>,
          props.clearButtonContainer,
        )}
    </div>
  );
};
