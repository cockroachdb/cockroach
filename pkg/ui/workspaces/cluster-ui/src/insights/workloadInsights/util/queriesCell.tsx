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
import { Tooltip } from "@cockroachlabs/ui-components";
import { limitStringArray } from "src/util";
import classNames from "classnames/bind";
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
    const query = transactionQueries?.length ? transactionQueries[0] : "";
    return <div>{query}</div>;
  }

  const combinedQuery = transactionQueries?.map((query, idx, arr) => (
    <div key={idx}>
      {idx != 0 && <br />}
      {query}
      {idx != arr.length - 1 && <br />}
    </div>
  ));

  const limitedText = limitStringArray(transactionQueries, 50);
  return (
    <Tooltip placement="bottom" content={<div>{combinedQuery}</div>}>
      <span className={cx("queries-row")}>{limitedText}</span>
    </Tooltip>
  );
}
