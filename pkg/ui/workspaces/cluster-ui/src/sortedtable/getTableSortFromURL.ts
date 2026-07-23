// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Location } from "history";

import { SortSetting } from "src/sortedtable";

export function getTableSortFromURL(location: Location): SortSetting | null {
  const { search } = location;
  const queryParams = new URLSearchParams(search);

  const ascending = queryParams.get("ascending");
  const columnTitle = queryParams.get("columnTitle");
  if (ascending == null || columnTitle == null) return null;

  return {
    ascending: ascending === "true",
    columnTitle: columnTitle,
  };
}
