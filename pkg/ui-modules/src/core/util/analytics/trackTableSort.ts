// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { analytics } from "src/redux/analytics";
import { SortableColumn, SortSetting } from "src/views/shared/components/sortabletable";

export const track = (fn: Function) => (
  name?: String,
  col?: SortableColumn,
  sortSetting?: SortSetting,
) => {
  const tableName = name || "";
  const columnName = col && col.title || "";
  const sortDirection = sortSetting && (sortSetting.ascending) ? "asc" : "desc";

  fn({
    event: "Table Sort",
    properties: {
      tableName,
      columnName,
      sortDirection,
    },
  });
};

export default function trackTableSort(
  name?: String,
  col?: SortableColumn,
  sortSetting?: SortSetting,
) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(name, col, sortSetting);
}
