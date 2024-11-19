// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Link } from "react-router-dom";

import { Anchor } from "src/anchor";
import { commonStyles } from "src/common";
import { EmptyTable, EmptyTableProps } from "src/empty";
import { statementsTable, tabAttr, viewAttr } from "src/util";

import emptyTableResultsImg from "../assets/emptyState/empty-table-results.svg";
import magnifyingGlassImg from "../assets/emptyState/magnifying-glass.svg";

import { StatementViewType } from "./statementPageTypes";

const footer = (
  <Anchor href={statementsTable} target="_blank">
    Learn more about statements
  </Anchor>
);

const emptySearchResults = {
  title: "No SQL statements match your search.",
  icon: magnifyingGlassImg,
  footer,
};

function getMessage(type: StatementViewType): EmptyTableProps {
  switch (type) {
    case StatementViewType.ACTIVE:
      return {
        title: "No active SQL statements",
        icon: emptyTableResultsImg,
        message: "There are currently no active statement executions.",
        footer: (
          <Link
            className={commonStyles("link")}
            to={`/sql-activity?${tabAttr}=Statements&${viewAttr}=fingerprints`}
          >
            View Statement Fingerprints to see historical statement statistics.
          </Link>
        ),
      };
    case StatementViewType.USING_INDEX:
      return {
        title:
          "No SQL statements using this index in the selected time interval",
        icon: emptyTableResultsImg,
        footer,
      };
    case StatementViewType.FINGERPRINTS:
    default:
      return {
        title: "No SQL statements in the selected time interval",
        icon: emptyTableResultsImg,
        footer,
      };
  }
}

export const EmptyStatementsPlaceholder: React.FC<{
  isEmptySearchResults: boolean;
  statementView: StatementViewType;
}> = ({ isEmptySearchResults, statementView }) => {
  const emptyPlaceholderProps: EmptyTableProps = isEmptySearchResults
    ? emptySearchResults
    : getMessage(statementView);
  return <EmptyTable {...emptyPlaceholderProps} />;
};
