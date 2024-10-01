// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React from "react";
import { Link } from "react-router-dom";

import { getHighlightedText } from "src/highlightedText";
import { limitText } from "src/util";

import statementsStyles from "../../statementsTable/statementsTableContent.module.scss";
import { TransactionLinkTarget } from "../transactionsTable";

import transactionsCellsStyles from "./transactionsCells.module.scss";

const statementsCx = classNames.bind(statementsStyles);
const ownCellStyles = classNames.bind(transactionsCellsStyles);
const descriptionClassName = statementsCx("cl-table-link__description");
const textWrapper = ownCellStyles("text-wrapper");
const hoverAreaClassName = ownCellStyles("hover-area");

interface TextCellProps {
  appName: string;
  transactionText: string;
  transactionSummary: string;
  transactionFingerprintId: string;
  search: string;
}

export const transactionLink = ({
  appName,
  transactionText,
  transactionSummary,
  transactionFingerprintId,
  search,
}: TextCellProps): React.ReactElement => {
  const linkProps = {
    application: appName,
    transactionFingerprintId,
  };

  return (
    <Link to={TransactionLinkTarget(linkProps)}>
      <div>
        <Tooltip
          placement="bottom"
          content={
            <pre className={descriptionClassName}>
              {getHighlightedText(
                transactionText,
                search,
                true /* hasDarkBkg */,
              )}
            </pre>
          }
        >
          <div className={textWrapper}>
            <div className={hoverAreaClassName}>
              {getHighlightedText(
                limitText(transactionSummary, 200),
                search,
                false /* hasDarkBkg */,
                true /* isOriginalText */,
              )}
            </div>
          </div>
        </Tooltip>
      </div>
    </Link>
  );
};
