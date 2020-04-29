// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Icon, Pagination } from "antd";
import * as React from "react";
import { getMatchParamByName } from "src/util/query";
import { match } from "react-router";

export interface PaginationSettings {
  pageSize?: number;
  current: number;
  total?: number;
}
export interface PaginationComponentProps {
  pagination: PaginationSettings;
  onChange: (current: number) => void;
  hideOnSinglePage?: boolean;
}

PaginationComponent.defaultProps = {
  pagination: {
    pageSize: 20,
    total: 0,
  },
};

export function PaginationComponent(props: PaginationComponentProps) {
  const { pagination, hideOnSinglePage, onChange } = props;
  const  renderPage = (_page: number, type: "page" | "prev" | "next" | "jump-prev" | "jump-next", originalElement: React.ReactNode) => {
    switch (type) {
      case "jump-prev":
        return (
          <div className="_pg-jump">
            <Icon type="left" />
            <span className="_jump-dots">•••</span>
          </div>
        );
      case "jump-next":
        return (
          <div className="_pg-jump">
            <Icon type="right" />
            <span className="_jump-dots">•••</span>
          </div>
        );
      default:
        return originalElement;
    }
  };
  return (
    <Pagination
      size="small"
      itemRender={renderPage as (page: number, type: "page" | "prev" | "next" | "jump-prev" | "jump-next") => React.ReactNode}
      pageSize={pagination.pageSize}
      current={pagination.current}
      total={pagination.total}
      onChange={onChange}
      hideOnSinglePage={hideOnSinglePage}
    />
  );
}

const getPageStart = (pageSize: number, current: number) => pageSize * current;

// tslint:disable-next-line: no-shadowed-variable
export const paginationPageCount = (pagination: PaginationSettings, pageName: string, match?: match<any>, appAttr?: string, search?: string) => {
  const { pageSize, current, total } = pagination;
  const appAttrValue = match && getMatchParamByName(match, appAttr);
  const selectedApp = appAttrValue || "";
  const start = Math.max(getPageStart(pageSize, current > 0 ? (current - 1) : current), 0);
  const recountedStart = total > 0 ? start + 1 : start;
  const end = Math.min(getPageStart(pageSize, current), total);
  const label = (search && search.length > 0) || selectedApp.length > 0 ? "results" : pageName;
  if (end === 0) {
    return `0 ${label}`;
  }
  return `${recountedStart}-${end} of ${total} ${label}`;
};
