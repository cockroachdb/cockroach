// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { EmptyTable, EmptyTableProps } from "src/empty";
import { Anchor } from "src/anchor";
import { insights } from "src/util";
import magnifyingGlassImg from "src/assets/emptyState/magnifying-glass.svg";
import emptyTableResultsImg from "src/assets/emptyState/empty-table-results.svg";

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
