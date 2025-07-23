import React from "react";
import { getHighlightedText, Anchor } from "src/index";
import { Tooltip2 as Tooltip } from "src/tooltip2";
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
const overlayClassName = statementsCx(
  "cl-table-link__statement-tooltip--fixed-width",
);

const textWrapper = ownCellStyles("text-wrapper");
const hoverAreaClassName = ownCellStyles("hover-area");

interface TextCellProps {
  transactionText: string;
  transactionIds: Long[];
  handleDetails: (transactionIds: Long[]) => void;
  search: string;
}

export const textCell = ({
  transactionText,
  transactionIds,
  handleDetails,
  search,
}: TextCellProps) => {
  const summary = summarize(transactionText);
  return (
    <div>
      <Tooltip
        placement="bottom"
        title={
          <pre className={descriptionClassName}>
            {getHighlightedText(transactionText, search)}
          </pre>
        }
        overlayClassName={overlayClassName}
      >
        <div className={textWrapper}>
          <div
            onClick={() => handleDetails(transactionIds)}
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
      title={
        <div className={statementsCx("tooltip__table--title")}>
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
      title={
        <div className={statementsCx("tooltip__table--title")}>
          <p>FILL THE TEXT</p>
        </div>
      }
    >
      Statements
    </Tooltip>
  ),
};
