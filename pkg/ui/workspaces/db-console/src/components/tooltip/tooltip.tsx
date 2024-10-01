// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  default as AntTooltip,
  TooltipProps as AntTooltipProps,
} from "antd/es/tooltip";
import cn from "classnames";
import * as React from "react";

import "./tooltip.styl";

export interface TooltipProps {
  children: React.ReactNode | string;
  theme?: "default" | "blue";
}

export function Tooltip(props: TooltipProps & AntTooltipProps) {
  const { children, theme, overlayClassName } = props;
  const classes = cn(
    "tooltip-overlay",
    `crl-tooltip--theme-${theme}`,
    overlayClassName,
  );
  return (
    <AntTooltip {...props} mouseEnterDelay={0.5} overlayClassName={classes}>
      {children}
    </AntTooltip>
  );
}

Tooltip.defaultProps = {
  theme: "default",
  placement: "top",
};
