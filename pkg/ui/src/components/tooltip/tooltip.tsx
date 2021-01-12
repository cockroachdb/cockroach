// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import {
  default as AntTooltip,
  TooltipProps as AntTooltipProps,
} from "antd/es/tooltip";
import cn from "classnames";

import "antd/es/tooltip/style/css";
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
