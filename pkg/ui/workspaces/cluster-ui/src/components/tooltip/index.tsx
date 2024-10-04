// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip as AntdTooltip, TooltipProps } from "antd";
import React from "react";

import styles from "./tooltip.module.scss";

type Props = TooltipProps & {
  noUnderline?: boolean;
};

export const Tooltip: React.FC<Props> = props => {
  const style = props.noUnderline ? "" : styles.underline;
  return (
    <AntdTooltip className={style} title={props.title}>
      {props.children}
    </AntdTooltip>
  );
};
