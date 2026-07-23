// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Spinner } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React from "react";

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
