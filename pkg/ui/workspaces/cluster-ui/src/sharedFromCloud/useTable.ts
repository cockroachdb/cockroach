// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { DateTime } from "luxon";
import { useEffect, useMemo, useReducer } from "react";
import { useHistory } from "react-router-dom";

import { parseURLParams, updateURLParams } from "./handleURLParams";

export interface Pagination {
  page: number;
  pageSize: number;
}

export interface Filters {
  [key: string]: string[];
}

export interface Sort {
  field: string;
  order: "asc" | "desc";
}

export interface TableParams {
  filters?: Filters;
  startingFrom?: DateTime;
  endingAt?: DateTime;
  pagination?: Pagination;
  search?: string;
  sort?: Sort;
}

type TableAction =
  | { type: "SET_FILTERS"; payload: Filters }
  | { type: "CLEAR_FILTERS" }
  | { type: "SET_STARTING_FROM"; payload: DateTime }
  | { type: "SET_ENDING_AT"; payload: DateTime }
  | { type: "SET_PAGINATION"; payload: Pagination }
  | { type: "SET_SEARCH"; payload: string }
  | { type: "SET_SORT"; payload: Sort };

const tableReducer = (
  params: TableParams,
  action: TableAction,
): TableParams => {
  switch (action.type) {
    case "SET_FILTERS":
      return {
        ...params,
        filters: {
          ...params.filters,
          ...action.payload,
        },
      };
    case "CLEAR_FILTERS":
      return {
        ...params,
        filters: {},
      };
    case "SET_STARTING_FROM":
      return {
        ...params,
        startingFrom: action.payload,
      };
    case "SET_ENDING_AT":
      return {
        ...params,
        endingAt: action.payload,
      };
    case "SET_PAGINATION":
      return {
        ...params,
        pagination: action.payload,
      };
    case "SET_SEARCH":
      return {
        ...params,
        search: action.payload,
      };
    case "SET_SORT":
      return {
        ...params,
        sort: action.payload,
      };
    default:
      return params;
  }
};

// URL params that are managed by useTable and should not be parsed as filters
export const managedURLParams = [
  "startingFrom",
  "endingAt",
  "page",
  "pageSize",
  "search",
  "sortField",
  "sortOrder",
];

export interface UseTableProps {
  initial?: TableParams;
  paramsToIgnore?: string[];
}

const emptyIgnoreParams = [] as string[];

/**
 * useTable manages the state of a table component, including filters, time range,
 * pagination, search, and sorting. It also updates the URL with the current parameters
 * of the table.
 *
 * @param initial the initial parameters applied to the table data
 * @param paramsToIgnore an array of URL search parameter names to ignore when updating the URL
 * @returns the current parameters of the table and functions to update these parameters
 * @example
 * const AuditLogsTable = () => {
 *   const initialParams: TableParams = {
 *     filters: { clusterId: ["cluster-1", "cluster-2"] },
 *     startingFrom: DateTime.fromSQL("2024-07-01 09:00:00").toUTC(),
 *     endingAt: DateTime.fromSQL("2024-07-03 09:00:00").toUTC(),
 *   };
 *
 *   const { params, setStartingFrom, setEndingAt } = useTable({initial: initialParams, paramsToIgnore: ["logId"]});
 *   const { data, isLoading, error, mutate } = useListAuditLogs(createListAuditLogsRequest(params));
 *
 *   const handleTimeRangeChange = (range: RangeValue<DateTime<boolean>>) => {
 *     setStartingFrom(range[0]);
 *     setEndingAt(range[1]);
 *   };
 *
 *   return (
 *     <LuxonDatePicker.Range
 *       value={[params.startingFrom, params.endingAt]}
 *       onChange={handleTimeRangeChange}
 *    />
 *    <Table ... />
 *   );
 * };
 */
const useTable = ({
  initial,
  paramsToIgnore = emptyIgnoreParams,
}: UseTableProps): {
  params: TableParams;
  setFilters: (filters: Filters) => void;
  clearFilters: () => void;
  setStartingFrom: (startingFrom: DateTime) => void;
  setEndingAt: (endingAt: DateTime) => void;
  setPagination: (pagination: Pagination) => void;
  setSearch: (search: string) => void;
  setSort: (sort: Sort) => void;
} => {
  const history = useHistory();
  const url = parseURLParams(history.location.search, paramsToIgnore);

  const initialParams: TableParams = {
    filters: url.filters ?? initial?.filters,
    startingFrom: url.startingFrom ?? initial?.startingFrom,
    endingAt: url.endingAt ?? initial?.endingAt,
    pagination: url.pagination ?? initial?.pagination,
    search: url.search ?? initial?.search,
    sort: url.sort ?? initial?.sort,
  };

  const [params, dispatch] = useReducer(tableReducer, initialParams);

  const setFilters = (filters: Filters) => {
    dispatch({ type: "SET_FILTERS", payload: filters });
  };

  const clearFilters = () => {
    dispatch({ type: "CLEAR_FILTERS" });
  };

  const setStartingFrom = (startingFrom: DateTime) => {
    dispatch({ type: "SET_STARTING_FROM", payload: startingFrom });
  };

  const setEndingAt = (endingAt: DateTime) => {
    dispatch({ type: "SET_ENDING_AT", payload: endingAt });
  };

  const setPagination = (pagination: Pagination) => {
    dispatch({ type: "SET_PAGINATION", payload: pagination });
  };

  const setSearch = (search: string) => {
    dispatch({ type: "SET_SEARCH", payload: search });
  };

  const setSort = (sort: Sort) => {
    dispatch({ type: "SET_SORT", payload: sort });
  };

  useEffect(() => {
    const newURLParams = updateURLParams(params);

    // Add any URL search parameters that should be ignored (i.e not touched)
    // to the new URLSearchParams object before replacing the URL
    const urlParams = new URLSearchParams(history.location.search);
    for (const param of paramsToIgnore) {
      const values = urlParams.getAll(param);
      for (const value of values) {
        newURLParams.append(param, value);
      }
    }

    history.replace({ search: newURLParams.toString() });
  }, [params, history, paramsToIgnore]);

  const memoizedParams = useMemo(() => params, [params]);

  return {
    params: memoizedParams,
    setFilters,
    clearFilters,
    setStartingFrom,
    setEndingAt,
    setPagination,
    setSearch,
    setSort,
  };
};

export default useTable;
