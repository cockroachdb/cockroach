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
  onClick: () => void;
  search: string;
}

export const textCell = ({
  transactionText,
  onClick,
  search,
}: TextCellProps): React.ReactElement => {
  const summary = summarize(transactionText);
  return (
    <div>
      <Tooltip
        placement="bottom"
        content={
          <pre className={descriptionClassName}>
            {getHighlightedText(transactionText, search, true)}
          </pre>
        }
      >
        <div className={textWrapper}>
          <div onClick={onClick} className={hoverAreaClassName}>
            {getHighlightedText(
              limitText(shortStatement(summary, transactionText), 200),
              search,
              false,
              true,
            )}
          </div>
        </div>
      </Tooltip>
    </div>
  );
};
