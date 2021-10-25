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
import { getHighlightedText } from "src/highlightedText";
import { Tooltip } from "@cockroachlabs/ui-components";
import { limitText } from "../utils";
import classNames from "classnames/bind";
import statementsStyles from "../../statementsTable/statementsTableContent.module.scss";
import transactionsCellsStyles from "./transactionsCells.module.scss";

const statementsCx = classNames.bind(statementsStyles);
const ownCellStyles = classNames.bind(transactionsCellsStyles);
const descriptionClassName = statementsCx("cl-table-link__description");

const textWrapper = ownCellStyles("text-wrapper");
const hoverAreaClassName = ownCellStyles("hover-area");
interface TextCellProps {
  transactionText: string;
  transactionSummary: string;
  onClick: () => void;
  search: string;
}

export const textCell = ({
  transactionText,
  transactionSummary,
  onClick,
  search,
}: TextCellProps): React.ReactElement => {
  return (
    <div>
      <Tooltip
        placement="bottom"
        content={
          <pre className={descriptionClassName}>
            {getHighlightedText(transactionText, search, true /* hasDarkBkg */)}
          </pre>
        }
      >
        <div className={textWrapper}>
          <div onClick={onClick} className={hoverAreaClassName}>
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
  );
};
