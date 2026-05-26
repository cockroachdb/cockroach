// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip as AntdTooltip, TooltipProps } from "antd";
import classNames from "classnames";
import React from "react";

import styles from "./tooltip.module.scss";

type Props = TooltipProps & {
  noUnderline?: boolean;
};

export const Tooltip: React.FC<Props> = props => {
  const underlineStyle = props.noUnderline ? "" : styles.underline;
  return (
    <AntdTooltip
      className={classNames(styles.container, underlineStyle)}
      title={<div className={styles.title}>{props.title}</div>}
    >
      {props.children}
    </AntdTooltip>
  );
};
