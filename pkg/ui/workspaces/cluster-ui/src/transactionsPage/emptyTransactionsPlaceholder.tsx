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
import { EmptyTable, EmptyTableProps } from "../empty";
import { Anchor } from "../anchor";
import { transactionsTable } from "../util";
import magnifyingGlassImg from "../assets/emptyState/magnifying-glass.svg";
import emptyTableResultsImg from "../assets/emptyState/empty-table-results.svg";

export const EmptyTransactionsPlaceholder: React.FC<{
  isEmptySearchResults: boolean;
}> = ({ isEmptySearchResults }) => {
  const footer = (
    <Anchor href={transactionsTable} target="_blank">
      Learn more about statements
    </Anchor>
  );

  const emptyPlaceholderProps: EmptyTableProps = isEmptySearchResults
    ? {
        title:
          "No transactions match your search since this page was last cleared",
        icon: magnifyingGlassImg,
        footer,
      }
    : {
        title: "No transactions since this page was last cleared",
        icon: emptyTableResultsImg,
        message:
          "Transactions are cleared every hour by default, or according to your configuration.",
        footer,
      };
  return <EmptyTable {...emptyPlaceholderProps} />;
};
