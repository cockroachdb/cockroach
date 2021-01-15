import * as React from "react";
import classNames from "classnames/bind";

import styles from "./badge.module.scss";

export type BadgeStatus = "success" | "danger" | "default" | "info" | "warning";

export interface BadgeProps {
  text: React.ReactNode;
  size?: "small" | "medium" | "large";
  status?: BadgeStatus;
  icon?: React.ReactNode;
  iconPosition?: "left" | "right";
}

const cx = classNames.bind(styles);

export function Badge(props: BadgeProps) {
  const { size, status, icon, iconPosition, text } = props;
  const classes = cx("badge", `badge--size-${size}`, `badge--status-${status}`);
  const iconClasses = cx(
    "badge__icon",
    `badge__icon--position-${iconPosition || "left"}`,
  );
  return (
    <div className={classes}>
      {icon && <div className={iconClasses}>{icon}</div>}
      <div className={cx("badge__text", "badge__text--no-wrap")}>{text}</div>
    </div>
  );
}

Badge.defaultProps = {
  size: "medium",
  status: "default",
};
