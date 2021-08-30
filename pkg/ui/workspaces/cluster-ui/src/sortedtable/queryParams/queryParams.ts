// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { History, Path, createPath } from "history";
import { ISortedTablePagination, SortSetting } from "../sortedtable";

export class QueryParams {
  constructor(private readonly history: History) {}

  get pagination(): ISortedTablePagination {
    return { current: this.page, pageSize: 20 };
  }

  set pagination(pagination: ISortedTablePagination) {
    this.history.push(
      this.pathWithUpdatedParams(params => {
        if (pagination.current == 1) {
          params.delete("page");
        } else {
          params.set("page", pagination.current.toString());
        }
      }),
    );
  }

  get sortSetting(): SortSetting {
    return { ascending: this.ascending, columnTitle: this.sortBy };
  }

  set sortSetting(sortSetting: SortSetting) {
    this.history.replace(
      this.pathWithUpdatedParams(params => {
        if (sortSetting.ascending) {
          params.delete("descending");
        } else {
          params.set("descending", "true");
        }

        if (sortSetting.columnTitle == null) {
          params.delete("sortBy");
        } else {
          params.set("sortBy", sortSetting.columnTitle);
        }
      }),
    );
  }

  private get ascending(): boolean {
    return !this.params.has("descending");
  }

  private get page(): number {
    return parseInt(this.params.get("page")) || 1;
  }

  private get sortBy(): string {
    return this.params.get("sortBy");
  }

  private get params(): URLSearchParams {
    return new URLSearchParams(this.history.location.search);
  }

  private pathWithUpdatedParams(
    updater: (params: URLSearchParams) => void,
  ): Path {
    const params = this.params;
    updater(params);
    params.sort();

    return createPath({
      pathname: this.history.location.pathname,
      search: params.toString(),
      hash: this.history.location.hash,
    });
  }
}
