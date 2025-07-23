import React from "react";
import AntPagination from "antd/lib/pagination";
import { PaginationProps as AntPaginationProps } from "antd/lib/pagination";
import classNames from "classnames/bind";
import styles from "./pagination.module.scss";

const cx = classNames.bind(styles);

export type PaginationProps = Pick<
  AntPaginationProps,
  "pageSize" | "current" | "total" | "onChange"
>;

export const Pagination: React.FC<PaginationProps> = props => {
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
    />
  );
};
