import React from "react";
import classnames from "classnames/bind";
import styles from "./pageConfig.module.scss";

export interface PageConfigProps {
  layout?: "list" | "spread";
  children?: React.ReactNode;
}

const cx = classnames.bind(styles);

export function PageConfig(props: PageConfigProps) {
  const classes = cx({
    "page-config__list": props.layout !== "spread",
    "page-config__spread": props.layout === "spread",
  });

  return (
    <div className={cx("page-config")}>
      <ul className={classes}>{props.children}</ul>
    </div>
  );
}

export interface PageConfigItemProps {
  children?: React.ReactNode;
}

export function PageConfigItem(props: PageConfigItemProps) {
  return <li className={cx("page-config__item")}>{props.children}</li>;
}
