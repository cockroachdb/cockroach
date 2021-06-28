// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import React from "react";
import { getHighlightedText } from "src/highlightedText";
import { Anchor } from "src/anchor";
import { Tooltip } from "@cockroachlabs/ui-components";
import { summarize } from "src/util/sql/summarize";
import { shortStatement } from "src/statementsTable/statementsTable";
import { statementsSql } from "src/util";
import { limitText } from "../utils";
import classNames from "classnames/bind";
import statementsStyles from "../../statementsTable/statementsTableContent.module.scss";
import transactionsCellsStyles from "./transactionsCells.module.scss";
import Long from "long";

const statementsCx = classNames.bind(statementsStyles);
const ownCellStyles = classNames.bind(transactionsCellsStyles);
const descriptionClassName = statementsCx("cl-table-link__description");

const textWrapper = ownCellStyles("text-wrapper");
const hoverAreaClassName = ownCellStyles("hover-area");

type TransactionStats = protos.cockroach.sql.ITransactionStatistics;

interface TextCellProps {
  transactionText: string;
  transactionFingerprintIds: Long[];
  transactionStats: TransactionStats;
  handleDetails: (
    transactionFingerprintIds: Long[],
    transactionStats: TransactionStats,
  ) => void;
  search: string;
}

export const textCell = ({
  transactionText,
  transactionFingerprintIds,
  transactionStats,
  handleDetails,
  search,
}: TextCellProps) => {
  const summary = summarize(transactionText);
  return (
    <div>
      <Tooltip
        placement="bottom"
        content={
          <pre className={descriptionClassName}>
            {getHighlightedText(transactionText, search)}
          </pre>
        }
      >
        <div className={textWrapper}>
          <div
            onClick={() =>
              handleDetails(transactionFingerprintIds, transactionStats)
            }
            className={hoverAreaClassName}
          >
            {getHighlightedText(
              limitText(shortStatement(summary, transactionText), 200),
              search,
              true,
            )}
          </div>
        </div>
      </Tooltip>
    </div>
  );
};

export const titleCells = {
  transactions: (
    <Tooltip
      placement="bottom"
      content={
        <div className={statementsCx("tooltip__table--content")}>
          <p>
            {"SQL statement "}
            <Anchor href={statementsSql} target="_blank">
              fingerprint.
            </Anchor>
          </p>
          <p>
            To view additional details of a SQL statement fingerprint, click
            this to open the Statement Details page.
          </p>
        </div>
      }
    >
      Transactions
    </Tooltip>
  ),

  statements: (
    <Tooltip
      placement="bottom"
      content={
        <div className={statementsCx("tooltip__table--content")}>
          <p>FILL THE TEXT</p>
        </div>
      }
    >
      Statements
    </Tooltip>
  ),
};
