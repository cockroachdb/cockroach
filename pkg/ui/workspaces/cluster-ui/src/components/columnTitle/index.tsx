// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "antd";
import React from "react";

import styles from "./columnTitle.module.scss";

type ColumnTitleProps = {
  withToolTip?: {
    tooltipText: React.ReactNode;
    placement?: "top" | "bottom" | "left" | "right";
  };
  title: React.ReactNode;
};

export const ColumnTitle: React.FC<ColumnTitleProps> = ({
  withToolTip,
  title,
}) => {
  return withToolTip ? (
    <Tooltip
      className={styles["title-with-tooltip"]}
      placement={withToolTip.placement ?? "top"}
      title={withToolTip.tooltipText}
    >
      {title}
    </Tooltip>
  ) : (
    <>{title}</>
  );
};
