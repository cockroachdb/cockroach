// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import { Anchor } from "src/anchor";
import emptyTableResultsImg from "src/assets/emptyState/empty-table-results.svg";
import magnifyingGlassImg from "src/assets/emptyState/magnifying-glass.svg";
import { EmptyTable, EmptyTableProps } from "src/empty";
import { sessionsTable } from "src/util";

const footer = (
  <Anchor href={sessionsTable} target="_blank">
    Learn more about sessions.
  </Anchor>
);

const emptySearchResults = {
  title: "No sessions match your search.",
  icon: magnifyingGlassImg,
};

export const EmptySessionsTablePlaceholder: React.FC<{
  isEmptySearchResults: boolean;
}> = props => {
  const emptyPlaceholderProps: EmptyTableProps = props.isEmptySearchResults
    ? emptySearchResults
    : {
        title: "No sessions currently running.",
        message:
          "Sessions show you which statements and transactions are running for the active session.",
        icon: emptyTableResultsImg,
        footer,
      };

  return <EmptyTable {...emptyPlaceholderProps} />;
};
