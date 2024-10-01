// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React from "react";

import { limitStringArray } from "src/util";

import styles from "./workloadInsights.module.scss";

const cx = classNames.bind(styles);

export function QueriesCell(
  transactionQueries: string[],
  textLimit: number,
): React.ReactElement {
  // Filter out null or undefined values from array
  if (transactionQueries) {
    transactionQueries = transactionQueries.filter(x => x);
  }
  if (
    !transactionQueries?.length ||
    (transactionQueries.length === 1 &&
      transactionQueries[0]?.length < textLimit)
  ) {
    const query = transactionQueries?.length
      ? transactionQueries[0]
      : "Query not available.";
    return <div>{query}</div>;
  }

  const combinedQuery = transactionQueries?.map((query, idx, arr) => (
    <div key={idx}>
      {idx !== 0 && <br />}
      {query}
      {idx !== arr.length - 1 && <br />}
    </div>
  ));

  const limitedText = limitStringArray(transactionQueries, 50);
  return (
    <Tooltip placement="bottom" content={<div>{combinedQuery}</div>}>
      <span className={cx("queries-row")}>{limitedText}</span>
    </Tooltip>
  );
}
