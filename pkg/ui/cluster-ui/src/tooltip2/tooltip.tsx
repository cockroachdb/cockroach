import * as React from "react";
import { Tooltip as AntTooltip } from "antd";
import { TooltipProps as AntTooltipProps } from "antd/lib/tooltip";
import classNames from "classnames/bind";
import styles from "./tooltip.module.scss";

export interface TooltipProps {
  children: React.ReactNode;
  theme?: "default" | "blue";
  tableTitle?: boolean;
}

const cx = classNames.bind(styles);

export const Tooltip = (props: TooltipProps & AntTooltipProps) => {
  const { children, theme, overlayClassName, tableTitle } = props;
  const classes = cx(
    "tooltip-overlay",
    `crl-tooltip--theme-${theme}`,
    overlayClassName,
  );

  const title = tableTitle ? (
    <div className={cx("tooltip__table--title")}>{props.title}</div>
  ) : (
    props.title
  );

  return (
    <AntTooltip
      {...props}
      title={title}
      mouseEnterDelay={0.5}
      overlayClassName={classes}
    >
      {children}
    </AntTooltip>
  );
};

Tooltip.defaultProps = {
  theme: "default",
  placement: "top",
  tableTitle: false,
};
