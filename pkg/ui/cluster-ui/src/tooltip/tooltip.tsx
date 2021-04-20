import React from "react";
import { Tooltip as AntTooltip } from "antd";
import { AbstractTooltipProps } from "antd/lib/tooltip";
import classNames from "classnames/bind";
import styles from "./tooltip.module.scss";

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
export const Tooltip = (props: ToolTipWrapperProps) => {
  const { text, children, placement } = props;
  const overlayClassName = cx(
    "tooltip__preset--white",
    `tooltip__preset--placement-${placement}`,
  );
  return (
    <AntTooltip
      title={text}
      placement="bottom"
      overlayClassName={overlayClassName}
      {...props}
    >
      {children}
    </AntTooltip>
  );
};

Tooltip.defaultProps = {
  placement: "bottom",
};
