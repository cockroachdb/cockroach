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
import { statisticsClasses } from "../transactionsPage/transactionsPageClasses";
import { ISortedTablePagination } from "../sortedtable";
import { Button } from "src/button";
import { ResultsPerPageLabel } from "src/pagination";
import classNames from "classnames/bind";
import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";
import { TimeScaleLabel } from "src/timeScaleDropdown/timeScaleLabel";
import { TimeScale } from "src/timeScaleDropdown";
import moment from "moment-timezone";

const { statistic, countTitle } = statisticsClasses;
const timeScaleStylesCx = classNames.bind(timeScaleStyles);

interface TableStatistics {
  pagination: ISortedTablePagination;
  totalCount: number;
  arrayItemName: string;
  activeFilters: number;
  search?: string;
  onClearFilters?: () => void;
  timeScale?: TimeScale;
  requestTime?: moment.Moment;
}

// TableStatistics shows statistics about the results being
// displayed on the table, including total results count,
// how many are currently being displayed on the page and
// if there is an active filter.
// This component has also a clear filter option.
export const TableStatistics: React.FC<TableStatistics> = ({
  pagination,
  totalCount = 0,
  search,
  arrayItemName,
  onClearFilters,
  activeFilters,
  timeScale,
  requestTime,
}) => {
  const periodLabel = timeScale && requestTime && (
    <>
      &nbsp;&nbsp;&nbsp;| &nbsp;
      <p className={timeScaleStylesCx("time-label")}>
        <TimeScaleLabel timeScale={timeScale} requestTime={requestTime} />
      </p>
    </>
  );

  const resultsPerPageLabel = (
    <>
      <ResultsPerPageLabel
        pagination={{ ...pagination, total: totalCount }}
        pageName={arrayItemName}
        search={search}
      />
      {periodLabel}
    </>
  );

  const clearBtn = (
    <>
      | &nbsp;
      <Button onClick={() => onClearFilters()} type="flat" size="small">
        Clear filters
      </Button>
    </>
  );

  const resultsCountAndClear = (
    <>
      {totalCount} {totalCount === 1 ? "result" : "results"}
      {periodLabel}
      &nbsp;&nbsp;&nbsp;{onClearFilters && clearBtn}
    </>
  );

  return (
    <div className={statistic}>
      <h4 className={countTitle}>
        {activeFilters ? resultsCountAndClear : resultsPerPageLabel}
      </h4>
    </div>
  );
};
