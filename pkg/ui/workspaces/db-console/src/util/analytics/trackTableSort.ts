// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { analytics } from "src/redux/analytics";

export const track =
  (fn: Function) =>
  (
    tableName = "",
    columnName = "",
    sortDirection: "asc" | "desc" = undefined,
  ) => {
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
  tableName?: string,
  columnTitle?: string,
  ascending?: boolean,
) {
  const boundTrack = analytics.track.bind(analytics);
  const sortDirection = ascending && ascending ? "asc" : "desc";
  track(boundTrack)(tableName, columnTitle, sortDirection);
}
