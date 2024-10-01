// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { DateTime } from "luxon";

import {
  Filters,
  Pagination,
  Sort,
  TableParams,
  managedURLParams,
} from "./useTable";

/**
 * Returns a URLSearchParams object based on the given TableParams object.
 * Intended to be used when updating the URL with the current TableParams.
 *
 * @param params - the TableParams to update the URL with
 * @returns a URLSearchParams object with the updated parameters
 */
export const updateURLParams = (params: TableParams) => {
  const { filters, startingFrom, endingAt, pagination, search, sort } = params;
  const urlSearchParams = new URLSearchParams();

  if (startingFrom) {
    const startingFromISO = startingFrom.toISO();
    if (startingFromISO) urlSearchParams.set("startingFrom", startingFromISO);
  }

  if (endingAt) {
    const endingAtISO = endingAt.toISO();
    if (endingAtISO) urlSearchParams.set("endingAt", endingAtISO);
  }

  if (pagination) {
    urlSearchParams.set("page", pagination.page.toString());
    urlSearchParams.set("pageSize", pagination.pageSize.toString());
  }

  if (search && search.trim() !== "") {
    urlSearchParams.set("search", search);
  }

  if (filters) {
    Object.entries(filters).forEach(([key, values]) => {
      if (values.length > 0) {
        values.forEach(value => urlSearchParams.append(key, value));
      }
    });
  }

  if (sort && sort.field !== "") {
    urlSearchParams.set("sortField", sort.field);
    urlSearchParams.set("sortOrder", sort.order);
  }

  return urlSearchParams;
};

// These assertion functions are used to validate the URL parameters, returning
// the parsed value if it is valid, otherwise returning undefined.
const assertFilters = (
  filters: Record<string, string[]>,
): Filters | undefined => {
  const validFilters: Filters = {};
  Object.entries(filters).forEach(([key, values]) => {
    if (Array.isArray(values) && values.every(v => typeof v === "string")) {
      validFilters[key] = values;
    }
  });
  return Object.keys(validFilters).length > 0 ? validFilters : undefined;
};

const assertDateTime = (dt: string | null): DateTime | undefined => {
  if (dt) {
    const dtObj = DateTime.fromISO(dt).toUTC();
    if (dtObj.isValid) return dtObj;
  }
  return undefined;
};

const assertSearch = (search: string | null): string | undefined => {
  return typeof search === "string" ? search : undefined;
};

const assertPagination = (
  page: string | null,
  pageSize: string | null,
): Pagination | undefined => {
  if (page && pageSize) {
    const p = parseInt(page, 10);
    const size = parseInt(pageSize, 10);
    if (!isNaN(p) && !isNaN(size) && p > 0 && size > 0) {
      return { page: p, pageSize: size };
    }
  }
  return undefined;
};

const assertSort = (
  sortField: string | null,
  sortOrder: string | null,
): Sort | undefined => {
  if (sortField && (sortOrder === "asc" || sortOrder === "desc")) {
    return { field: sortField, order: sortOrder };
  }
  return undefined;
};

/**
 * Parses the URL parameters and returns a TableParams object with the
 * parsed values. If a parameter is not present or is invalid, it is
 * omitted from the returned object. Intended to be used when the page
 * is first loaded.
 *
 * @param url the URL to parse TableParams from
 * @param paramsToIgnore an array of URL search param names to ignore when parsing
 * @returns a TableParams object with the parsed values
 */
export const parseURLParams = (url: string, paramsToIgnore: string[] = []) => {
  const urlSearchParams = new URLSearchParams(url);
  const params: TableParams = {};

  // These parameters are not filters and should not be included in the filters object
  const nonFilterParamKeys = new Set([...managedURLParams, ...paramsToIgnore]);

  const rawFilters: Record<string, string[]> = {};
  urlSearchParams.forEach((value, key) => {
    if (!nonFilterParamKeys.has(key)) {
      if (rawFilters[key]) {
        rawFilters[key].push(value);
      } else {
        rawFilters[key] = [value];
      }
    }
  });

  params.filters = assertFilters(rawFilters);
  params.startingFrom = assertDateTime(urlSearchParams.get("startingFrom"));
  params.endingAt = assertDateTime(urlSearchParams.get("endingAt"));
  params.search = assertSearch(urlSearchParams.get("search"));
  params.pagination = assertPagination(
    urlSearchParams.get("page"),
    urlSearchParams.get("pageSize"),
  );
  params.sort = assertSort(
    urlSearchParams.get("sortField"),
    urlSearchParams.get("sortOrder"),
  );

  return params;
};
