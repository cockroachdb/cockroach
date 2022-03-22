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
import { Tooltip } from "@cockroachlabs/ui-components";
import {
  contentModifiers,
  StatisticType,
} from "../statsTableUtil/statsTableUtil";
import classNames from "classnames/bind";
import styles from "./sqlActivity.module.scss";

const cx = classNames.bind(styles);

interface clearStatsProps {
  resetSQLStats: () => void;
  tooltipType: StatisticType;
}

const ClearStats = (props: clearStatsProps): React.ReactElement => {
  let statsType = "";
  switch (props.tooltipType) {
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
  const toolTipText = `${statsType} statistics are aggregated once an hour by default and organized by the start time. 
  Statistics between two hourly intervals belong to the nearest hour rounded down. 
  For example, a ${statsType} execution ending at 1:50 would have its statistics aggregated in the 1:00 interval 
  start time. Clicking ‘reset SQL stats’ will reset SQL stats on the Statements and Transactions pages and 
  crdb_internal tables.`;
  return (
    <Tooltip content={toolTipText} style="tableTitle">
      <a
        className={cx("action", "tooltip-info", "separator")}
        onClick={props.resetSQLStats}
      >
        reset SQL stats
      </a>
    </Tooltip>
  );
};

export default ClearStats;
