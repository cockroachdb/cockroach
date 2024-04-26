// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import {
  Pagination as AntPagination,
  PaginationProps as AntPaginationProps,
} from "antd";
import classNames from "classnames/bind";

import styles from "./pagination.module.scss";

const cx = classNames.bind(styles);

export const Pagination: React.FC<AntPaginationProps> = props => {
  const itemRenderer = React.useCallback(
    (
      _page: number,
      type: "page" | "prev" | "next",
      originalElement: React.ReactNode,
    ) => {
      switch (type) {
        case "prev":
        case "next":
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
