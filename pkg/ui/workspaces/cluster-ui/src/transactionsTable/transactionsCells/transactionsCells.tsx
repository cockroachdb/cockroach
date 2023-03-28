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
import { Link } from "react-router-dom";
import { getHighlightedText } from "src/highlightedText";
import { Tooltip } from "@cockroachlabs/ui-components";
import { limitText, unset } from "src/util";
import classNames from "classnames/bind";
import statementsStyles from "../../statementsTable/statementsTableContent.module.scss";
import transactionsCellsStyles from "./transactionsCells.module.scss";
import { TransactionLinkTarget } from "../transactionsTable";

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
