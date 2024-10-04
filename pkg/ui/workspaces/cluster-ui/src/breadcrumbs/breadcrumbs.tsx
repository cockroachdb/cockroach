// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { FunctionComponent, ReactElement } from "react";
import { Link } from "react-router-dom";
import classnames from "classnames/bind";
import styles from "./breadcrumbs.module.scss";

export interface BreadcrumbItem {
  name: string;
  link: string;
  onClick?: () => void;
}

interface BreadcrumbsProps {
  items: BreadcrumbItem[];
  divider?: ReactElement;
}

const cx = classnames.bind(styles);

export const Breadcrumbs: FunctionComponent<BreadcrumbsProps> = ({
  items,
  divider = "/",
}) => {
  if (items.length === 0) {
    return null;
  }
  const lastItem = items.pop();
  return (
    <div className={cx("breadcrumbs")}>
      {items.map(({ link, name, onClick = () => {} }) => (
        <div className={cx("breadcrumbs__item")} key={link}>
          <Link
            className={cx("breadcrumbs__item--link")}
            to={link}
            onClick={onClick}
          >
            {name}
          </Link>
          <span className={cx("breadcrumbs__item--divider")}>{divider}</span>
        </div>
      ))}
      <div className={cx("breadcrumbs__item", "breadcrumbs__item--last")}>
        {lastItem?.name}
      </div>
    </div>
  );
};
