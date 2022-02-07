// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { useQueryParmeters } from "../hooks/useQueryParameters";
import { SortSetting } from "src/sortedtable";

export function useTableSortFromURL(defaultValue: SortSetting): SortSetting {
  const queryParams = useQueryParmeters();
  const ascending =
    (queryParams.get("ascending") || defaultValue.ascending) === "true";
  const columnTitle = queryParams.get("columnTitle");
  return {
    ascending: ascending,
    columnTitle: columnTitle || defaultValue.columnTitle,
  };
}
