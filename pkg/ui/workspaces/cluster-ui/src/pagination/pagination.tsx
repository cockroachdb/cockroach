// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Pagination as AntPagination,
  PaginationProps as AntPaginationProps,
} from "antd";
import classNames from "classnames/bind";
import React from "react";

import styles from "./pagination.module.scss";

const cx = classNames.bind(styles);

export const Pagination: React.FC<AntPaginationProps> = props => {
  const itemRenderer = React.useCallback(
    (
      _page: number,
      type: "page" | "prev" | "next" | "jump-prev" | "jump-next",
      originalElement: React.ReactNode,
    ) => {
      switch (type) {
        case "jump-prev":
        case "jump-next":
          return (
            <div className={cx("_pg-jump")}>
              <span className={cx("_jump-dots")}>•••</span>
            </div>
          );
        default:
          return originalElement;
      }
    },
    [],
  );

  return (
    <AntPagination
      {...props}
      size="small"
      itemRender={itemRenderer}
      hideOnSinglePage
      className={cx("root")}
    />
  );
};
