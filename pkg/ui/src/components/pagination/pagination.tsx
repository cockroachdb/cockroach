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
