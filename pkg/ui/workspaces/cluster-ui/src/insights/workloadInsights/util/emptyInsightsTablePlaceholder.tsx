// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import { Anchor } from "src/anchor";
import emptyTableResultsImg from "src/assets/emptyState/empty-table-results.svg";
import magnifyingGlassImg from "src/assets/emptyState/magnifying-glass.svg";
import { EmptyTable, EmptyTableProps } from "src/empty";
import { insights } from "src/util";

const footer = (
  <Anchor href={insights} target="_blank">
    Learn more about insights.
  </Anchor>
);

const emptySearchResults = {
  title: "No insight events match your search.",
  icon: magnifyingGlassImg,
  footer,
};

export const EmptyInsightsTablePlaceholder: React.FC<{
  isEmptySearchResults: boolean;
}> = props => {
  const emptyPlaceholderProps: EmptyTableProps = props.isEmptySearchResults
    ? emptySearchResults
    : {
        title: "No insight events since this page was last refreshed.",
        icon: emptyTableResultsImg,
        footer,
      };

  return <EmptyTable {...emptyPlaceholderProps} />;
};
