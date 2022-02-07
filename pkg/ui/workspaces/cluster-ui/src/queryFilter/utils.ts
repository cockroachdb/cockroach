// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Filters, defaultFilters } from ".";
import { Location } from "history";
import { ActiveStatementFilters, ActiveTransactionFilters } from "src";

export function getFiltersFromURL(location: Location): Partial<Filters> | null {
  const { search } = location;
  const queryParams = new URLSearchParams(search);
  const filters: Filters = {};

  Object.keys(defaultFilters).forEach((key: string) => {
    const param = queryParams.get(key);
    if (param == null) {
      return;
    }

    filters[key] = typeof param === "boolean" ? param === "true" : param;
  });

  // If we don't find any filters defined in the URL, we will return null.
  if (Object.keys(filters).length === 0) {
    return null;
  }

  return filters;
}

export function getActiveStatementFiltersFromURL(
  location: Location,
): Partial<ActiveStatementFilters> | null {
  const filters = getFiltersFromURL(location);
  if (!filters) return null;

  const appFilters = {
    app: filters.app,
  };

  // If every entry is null, there were no active stmt filters. Return null.
  if (Object.values(appFilters).every(val => !val)) return null;

  return appFilters;
}

export function getActiveTransactionFiltersFromURL(
  location: Location,
): Partial<ActiveTransactionFilters> | null {
  const filters = getFiltersFromURL(location);
  if (!filters) return null;

  const appFilters = {
    app: filters.app,
  };

  // If every entry is null, there were no active stmt filters. Return null.
  if (Object.values(appFilters).every(val => !val)) return null;

  return appFilters;
}
