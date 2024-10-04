// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { EmptyTable, EmptyTableProps } from "../empty";
import { Anchor } from "../anchor";
import { transactionsTable } from "../util";
import magnifyingGlassImg from "../assets/emptyState/magnifying-glass.svg";
import emptyTableResultsImg from "../assets/emptyState/empty-table-results.svg";
import { TransactionViewType } from "./transactionsPageTypes";
import { tabAttr, viewAttr } from "src/util";
import { Link } from "react-router-dom";
import { commonStyles } from "src/common";

const footer = (
  <Anchor href={transactionsTable} target="_blank">
    Learn more about transactions
  </Anchor>
);

const emptySearchResults = {
  title: "No transactions match your search.",
  icon: magnifyingGlassImg,
  footer,
};

function getMessage(type: TransactionViewType): EmptyTableProps {
  switch (type) {
    case TransactionViewType.ACTIVE:
      return {
        title: "No active SQL transactions",
        icon: emptyTableResultsImg,
        message: "There are currently no active transaction executions.",
        footer: (
          <Link
            className={commonStyles("link")}
            to={`/sql-activity?${tabAttr}=Transactions&${viewAttr}=fingerprints`}
          >
            View Transaction Fingerprints to see historical transaction
            statistics.
          </Link>
        ),
      };
    case TransactionViewType.FINGERPRINTS:
    default:
      return {
        title: "No transactions in the selected time interval",
        icon: emptyTableResultsImg,
        footer,
      };
  }
}

export const EmptyTransactionsPlaceholder: React.FC<{
  isEmptySearchResults: boolean;
  transactionView: TransactionViewType;
}> = ({ isEmptySearchResults, transactionView }) => {
  const emptyPlaceholderProps: EmptyTableProps = isEmptySearchResults
    ? emptySearchResults
    : getMessage(transactionView);

  return <EmptyTable {...emptyPlaceholderProps} />;
};
