// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
