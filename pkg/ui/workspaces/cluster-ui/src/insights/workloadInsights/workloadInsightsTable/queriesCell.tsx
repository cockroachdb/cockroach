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
import { limitText } from "src/util";
import classNames from "classnames/bind";
import styles from "./insightTable.module.scss";

const cx = classNames.bind(styles);

interface QueriesCellProps {
  transactionQueries: string[];
  textLimit: number;
}

export const QueriesCell = ({
  transactionQueries,
  textLimit,
}: QueriesCellProps): React.ReactElement => {
  if (
    transactionQueries.length < 2 &&
    transactionQueries[0].length < textLimit
  ) {
    return <div>{transactionQueries[0]}</div>;
  } else {
    const limitedText = limitText(transactionQueries[0], textLimit);
    return (
      <Tooltip
        placement="bottom"
        content={
          <div>
            {transactionQueries.map(query => (
              <div>{query}</div>
            ))}
          </div>
        }
      >
        <div className={cx("queries-row")}>{limitedText}</div>
      </Tooltip>
    );
  }
};
