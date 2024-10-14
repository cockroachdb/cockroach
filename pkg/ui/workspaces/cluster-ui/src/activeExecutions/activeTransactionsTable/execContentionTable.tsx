// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import React from "react";
import { Link } from "react-router-dom";

import { ColumnDescriptor, SortedTable } from "../../sortedtable";
import { Timestamp } from "../../timestamp";
import { DATE_FORMAT_24_TZ, Duration, limitText } from "../../util";
import { executionsTableTitles } from "../execTableCommon";
import { StatusIcon } from "../statusIcon";
import { ContendedExecution, ExecutionType } from "../types";

const getID = (item: ContendedExecution, execType: ExecutionType) =>
  execType === "transaction"
    ? item.transactionExecutionID
    : item.statementExecutionID;

export function makeContentionColumns(
  execType: ExecutionType,
): ColumnDescriptor<ContendedExecution>[] {
  const columns: ColumnDescriptor<ContendedExecution | null>[] = [
    {
      name: "executionID",
      title: executionsTableTitles.executionID(execType),
      cell: item => (
        <Link
          to={`/execution/${execType.toLowerCase()}/${getID(item, execType)}`}
        >
          {getID(item, execType)}
        </Link>
      ),
      sort: item => getID(item, execType),
      alwaysShow: true,
    },
    {
      name: "mostRecentStatement",
      title: executionsTableTitles.mostRecentStatement(execType),
      cell: item => (
        <Tooltip placement="bottom" content={item.query}>
          <Link to={`/execution/statement/${item.statementExecutionID}`}>
            {limitText(item.query, 50)}
          </Link>
        </Tooltip>
      ),
      sort: item => item.query,
    },
    execType === "statement"
      ? {
          name: "transactionID",
          title: executionsTableTitles.executionID("transaction"),
          cell: item => (
            <Link to={`/execution/transaction/${item.transactionExecutionID}`}>
              {item.transactionExecutionID}
            </Link>
          ),
          sort: item => item.transactionExecutionID,
          alwaysShow: true,
        }
      : null,
    {
      name: "status",
      title: executionsTableTitles.status(execType),
      cell: item => (
        <span>
          <StatusIcon status={item.status} />
          {item.status}
        </span>
      ),
      sort: item => item.status,
    },
    {
      name: "startTime",
      title: executionsTableTitles.startTime(execType),
      cell: item => <Timestamp time={item.start} format={DATE_FORMAT_24_TZ} />,
      sort: item => item.start.unix(),
    },
    {
      name: "timeSpentBlocking",
      title: executionsTableTitles.timeSpentBlocking(execType),
      cell: item => Duration(item.contentionTime.asSeconds() * 1e9),
      sort: item => item.contentionTime.asSeconds(),
    },
  ];
  return columns.filter(col => col);
}

interface ContentionTableProps {
  data: ContendedExecution[];
  execType: ExecutionType;
}

const txnColumns = makeContentionColumns("transaction");
const stmtColumns = makeContentionColumns("statement");

export const ExecutionContentionTable: React.FC<
  ContentionTableProps
> = props => {
  const columns = props.execType === "statement" ? stmtColumns : txnColumns;
  return <SortedTable columns={columns} {...props} />;
};
