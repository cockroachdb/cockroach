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
import tableStatsStyles from "./tableStatistics.module.scss";
import classNames from "classnames/bind";
import { Icon } from "@cockroachlabs/ui-components";
import {
  contentModifiers,
  StatisticType,
} from "../statsTableUtil/statsTableUtil";

const { statistic, countTitle, lastCleared } = statisticsClasses;
const cxStats = classNames.bind(tableStatsStyles);

interface TableStatistics {
  pagination: ISortedTablePagination;
  totalCount: number;
  lastReset: Date | string;
  arrayItemName: string;
  tooltipType: StatisticType;
  activeFilters: number;
  search?: string;
  onClearFilters?: () => void;
  resetSQLStats: () => void;
}

export const TableStatistics: React.FC<TableStatistics> = ({
  pagination,
  totalCount,
  lastReset,
  search,
  arrayItemName,
  tooltipType,
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

  let statsType = "";
  switch (tooltipType) {
    case "transaction":
      statsType = contentModifiers.transactionCapital;
      break;
    case "statement":
      statsType = contentModifiers.statementCapital;
      break;
    case "transactionDetails":
      statsType = contentModifiers.statementCapital;
      break;
    default:
      break;
  }
  const toolTipText = `${statsType} statistics are aggregated once an hour and organized by the start time. 
  Statistics between two hourly intervals belong to the nearest hour rounded down. 
  For example, a ${statsType} execution ending at 1:50 would have its statistics aggregated in the 1:00 interval 
  start time. Clicking ‘clear SQL stats’ will reset SQL stats on the Statements and Transactions pages and 
  crdb_internal tables.`;

  return (
    <div className={statistic}>
      <h4 className={countTitle}>
        {activeFilters ? resultsCountAndClear : resultsPerPageLabel}
      </h4>
      <div className={cxStats("flex-display")}>
        <Tooltip content={toolTipText} style="tableTitle">
          <div className={cxStats("tooltip-hover-area")}>
            <Icon iconName={"InfoCircle"} />
          </div>
        </Tooltip>
        <div className={lastCleared}>
          <a className={cxStats("action")} onClick={resetSQLStats}>
            clear SQL stats
          </a>
        </div>
      </div>
    </div>
  );
};
