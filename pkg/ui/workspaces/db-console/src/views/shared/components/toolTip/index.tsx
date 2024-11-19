// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "antd";
import classNames from "classnames/bind";
import React from "react";

import styles from "./tooltip.module.styl";

import type { AbstractTooltipProps } from "antd/lib/tooltip";

interface ToolTipWrapperProps extends AbstractTooltipProps {
  text: React.ReactNode;
  short?: boolean;
  children?: React.ReactNode;
}

const cx = classNames.bind(styles);

/**
 * ToolTipWrapper wraps its children with an area that detects mouseover events
 * and, when hovered, displays a floating tooltip to the immediate right of
 * the wrapped element.
 *
 * Note that the child element itself must be wrappable; certain CSS attributes
 * such as "float" will render parent elements unable to properly wrap their
 * contents.
 */

export const ToolTipWrapper = (props: ToolTipWrapperProps) => {
  const { text, children, placement = "bottom" } = props;
  const overlayClassName = cx("tooltip-wrapper", "tooltip__preset--white");
  return (
    <Tooltip
      title={text}
      placement={placement}
      overlayClassName={overlayClassName}
      {...props}
    >
      {children}
    </Tooltip>
  );
};

ToolTipWrapper.defaultProps = {
  placement: "bottom",
};
