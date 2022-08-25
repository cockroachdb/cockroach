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
import magnifyingGlassImg from "src/assets/emptyState/magnifying-glass.svg";
import emptyTableResultsImg from "src/assets/emptyState/empty-table-results.svg";

const emptySearchResults = {
  title: "No schema insight match your search.",
  icon: magnifyingGlassImg,
};

export const EmptySchemaInsightsTablePlaceholder: React.FC<{
  isEmptySearchResults: boolean;
}> = props => {
  const emptyPlaceholderProps: EmptyTableProps = props.isEmptySearchResults
    ? emptySearchResults
    : {
        title: "No schema insight since this page was last refreshed.",
        icon: emptyTableResultsImg,
      };

  return <EmptyTable {...emptyPlaceholderProps} />;
};
