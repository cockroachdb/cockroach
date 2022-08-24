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
import { Spinner } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import styles from "./tableSpinner.module.scss";

const cx = classNames.bind(styles);

interface TableSpinnerProps {
  loadingLabel: string;
}

export const TableSpinner = ({
  loadingLabel,
}: TableSpinnerProps): React.ReactElement => {
  const tableSpinnerClass = cx("table__loading");
  const spinClass = cx("table__loading--spin");
  const loadingLabelClass = cx("table__loading--label");

  return (
    <div className={tableSpinnerClass}>
      <Spinner className={spinClass} />
      {loadingLabel && (
        <span className={loadingLabelClass}>{loadingLabel}</span>
      )}
    </div>
  );
};
