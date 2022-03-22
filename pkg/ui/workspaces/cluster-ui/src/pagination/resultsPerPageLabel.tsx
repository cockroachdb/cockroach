// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

export interface PaginationSettings {
  pageSize?: number;
  current: number;
  total?: number;
}

export interface ResultsPerPageLabelProps {
  pagination: PaginationSettings;
  pageName: string;
  selectedApp?: string;
  search?: string;
}

export const ResultsPerPageLabel: React.FC<ResultsPerPageLabelProps> = ({
  pagination: { pageSize, current, total },
  pageName,
  selectedApp = "",
  search,
}) => {
  const getPageStart = (pageSize: number, current: number) =>
    pageSize * current;
  const start = Math.max(
    getPageStart(pageSize, current > 0 ? current - 1 : current),
    0,
  );
  const recountedStart = total > 0 ? start + 1 : start;
  const end = Math.min(getPageStart(pageSize, current), total);
  const label =
    (search && search.length > 0) || selectedApp.length > 0
      ? "results"
      : pageName;
  if (end === 0) {
    return <>{`0 ${label}`}</>;
  }
  return <>{`${recountedStart}-${end} of ${total} ${label}`}</>;
};
