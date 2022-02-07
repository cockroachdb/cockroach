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
import { useQueryParmeters } from "../hooks/useQueryParameters";

export function useFiltersFromURL(
  defaultValues?: Partial<Filters>,
): Partial<Filters> {
  const queryParams = useQueryParmeters();
  const filters: Filters = {};

  Object.keys(defaultFilters).forEach((key: string) => {
    const param = queryParams.get(key);
    if (param != null) {
      filters[key] = typeof param === "boolean" ? param === "true" : param;
    } else if (defaultValues != null && defaultValues[key] != null) {
      filters[key] = defaultValues[key];
    }
  });

  return filters;
}
