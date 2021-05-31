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
import moment from "moment";
import { DATE_FORMAT } from "src/util";
import { statisticsClasses } from "../transactionsPage/transactionsPageClasses";
import { ISortedTablePagination } from "../sortedtable";
import { Button } from "src/button";
import { ResultsPerPageLabel } from "src/pagination";
import { Tooltip } from "@cockroachlabs/ui-components";
import statementStyles from "src/statementDetails/statementDetails.module.scss";
import tableStatsStyles from "./tableStatistics.module.scss";
import classNames from "classnames/bind";
import { Icon } from "@cockroachlabs/ui-components";

const { statistic, countTitle, lastCleared } = statisticsClasses;
const cxStmt = classNames.bind(statementStyles);
const cxStats = classNames.bind(tableStatsStyles);

interface TableStatistics {
  pagination: ISortedTablePagination;
  totalCount: number;
  lastReset: Date | string;
  arrayItemName: string;
  activeFilters: number;
  search?: string;
  onClearFilters?: () => void;
  resetSQLStats: () => void;
}

const toolTipText = `Statement history is cleared once an hour by default, which can be configured with the cluster setting
 diagnostics.reporting.interval. Clicking ‘Clear SQL stats’ will reset SQL stats on the statements and transactions pages.`;

const renderLastCleared = (lastReset: string | Date) => {
  return `Last cleared ${moment.utc(lastReset).format(DATE_FORMAT)}`;
};

export const TableStatistics: React.FC<TableStatistics> = ({
  pagination,
  totalCount,
  lastReset,
  search,
  arrayItemName,
  onClearFilters,
  activeFilters,
  resetSQLStats,
}) => {
  const resultsPerPageLabel = (
    <ResultsPerPageLabel
      pagination={{ ...pagination, total: totalCount }}
      pageName={arrayItemName}
      search={search}
    />
  );

  const resultsCountAndClear = (
    <>
      {totalCount} {totalCount === 1 ? "result" : "results"}
      &nbsp;&nbsp;&nbsp;| &nbsp;
      <Button onClick={() => onClearFilters()} type="flat" size="small">
        clear filter
      </Button>
    </>
  );

  return (
    <div className={statistic}>
      <h4 className={countTitle}>
        {activeFilters ? resultsCountAndClear : resultsPerPageLabel}
      </h4>
      <div className={cxStats("flex-display")}>
        <Tooltip content={toolTipText}>
          <div className={cxStats("tooltip-hover-area")}>
            <Icon iconName={"InfoCircle"} />
          </div>
        </Tooltip>
        <div className={lastCleared}>
          {renderLastCleared(lastReset)}
          {"  "}-{"  "}
          <a onClick={resetSQLStats}>Clear SQL Stats</a>
        </div>
      </div>
    </div>
  );
};
