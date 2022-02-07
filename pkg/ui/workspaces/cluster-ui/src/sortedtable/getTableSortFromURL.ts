// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
