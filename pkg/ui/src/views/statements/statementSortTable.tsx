// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Icon, Pagination } from "antd";
import moment from "moment";
import React from "react";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { appAttr } from "src/util/constants";
import { DATE_FORMAT } from "src/util/format";
import { getMatchParamByName } from "src/util/query";
import { SortSetting } from "src/views/shared/components/sortabletable";
import Empty from "../app/components/empty";
import "./statements.styl";
import { AggregateStatistics, makeStatementsColumns, StatementsSortedTable } from "./statementsTable";
import { merge, forIn } from "lodash";
import { trackSearch, trackPaginate } from "src/util/analytics";
import { ActivateDiagnosticsModalRef } from "./diagnostics/activateDiagnosticsModal";

export interface StatementSortTableProps {
  statements: AggregateStatistics[];
  lastReset: string;
  search?: string;
  activateDiagnosticsRef?: React.RefObject<ActivateDiagnosticsModalRef>;
}

interface PaginationSettings {
  pageSize: number;
  current: number;
}

export interface StatementSortTableState {
  sortSetting: SortSetting;
  pagination: PaginationSettings;
}

export class StatementSortTable extends React.Component<StatementSortTableProps & RouteComponentProps<any>, StatementSortTableState> {

  constructor(props: StatementSortTableProps & RouteComponentProps<any>) {
    super(props);
    const defaultState = {
      sortSetting: {
        sortKey: 3, // Sort by Execution Count column as default option
        ascending: false,
      },
      pagination: {
        pageSize: 20,
        current: 1,
      },
    };

    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(defaultState, stateFromHistory);
  }

  getStateFromHistory = (): Partial<StatementSortTableState> => {
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const sortKey = searchParams.get("sortKey") || undefined;
    const ascending = searchParams.get("ascending") || undefined;

    return {
      sortSetting: {
        sortKey,
        ascending: Boolean(ascending),
      },
    };
  }

  syncHistory = (params: Record<string, string | undefined>) => {
    const { history } = this.props;
    const currentSearchParams = new URLSearchParams(history.location.search);

    forIn(params, (value, key) => {
      if (!value) {
        currentSearchParams.delete(key);
      } else {
        currentSearchParams.set(key, value);
      }
    });

    history.location.search = currentSearchParams.toString();
    history.replace(history.location);
  }

  componentDidUpdate = (prevProps: StatementSortTableProps & RouteComponentProps<any>) => {
    if (this.props.search && this.props.search !== prevProps.search) {
      trackSearch(this.filteredStatementsData().length);
    }
  }

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
    this.syncHistory({
      "sortKey": ss.sortKey,
      "ascending": Boolean(ss.ascending).toString(),
    });
  }

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current }});
    trackPaginate(current);
  }

  getStatementsData = () => {
    const { pagination: { current, pageSize } } = this.state;
    const currentDefault = current - 1;
    const start = (currentDefault * pageSize);
    const end = (currentDefault * pageSize + pageSize);
    const data = this.filteredStatementsData().slice(start, end);
    return data;
  }

  filteredStatementsData = () => {
    const { statements, search } = this.props;
    if (search) {
      return statements.filter(statement => search.split(" ").every(val => statement.label.toLowerCase().includes(val.toLowerCase())));
    }
    return statements;
  }

  renderPage = (_page: number, type: "page" | "prev" | "next" | "jump-prev" | "jump-next", originalElement: React.ReactNode) => {
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
  }

  renderCounts = () => {
    const { pagination: { current, pageSize } } = this.state;
    const { match, search } = this.props;
    const appAttrValue = getMatchParamByName(match, appAttr);
    const selectedApp = appAttrValue || "";
    const total = this.filteredStatementsData().length;
    const pageCount = current * pageSize > total ? total : current * pageSize;
    const count = total > 10 ? pageCount : current * total;
    if (search && search.length > 0) {
      const text = `${total} ${total > 1 || total === 0 ? "results" : "result"} for`;
      const filter = selectedApp ? <React.Fragment>in <span className="label">{selectedApp}</span></React.Fragment> : null;
      return (
        <React.Fragment>{text} <span className="label">{search}</span> {filter}</React.Fragment>
      );
    }
    return `${count} of ${total} statements`;
  }

  renderLastCleared = () => {
    const { lastReset } = this.props;
    return `Last cleared ${moment.utc(lastReset).format(DATE_FORMAT)}`;
  }

  noStatementResult = () => (
    <>
      <p>There are no SQL statements that match your search or filter since this page was last cleared.</p>
      <a href="https://www.cockroachlabs.com/docs/stable/admin-ui-statements-page.html" target="_blank">Learn more about the statement page</a>
    </>
  )

  render() {
    const { pagination } = this.state;
    const { statements, match, search, activateDiagnosticsRef } = this.props;
    const appAttrValue = getMatchParamByName(match, appAttr);
    const selectedApp = appAttrValue || "";
    const data = this.getStatementsData();
    const isEmptyTable = search ? data.length === 0 && search.length === 0 : data.length === 0;
    return (
      <>
        <section className="cl-table-container">
          <div className="cl-table-statistic">
            <h4 className="cl-count-title">
              {this.renderCounts()}
            </h4>
            <h4 className="last-cleared-title">
              {this.renderLastCleared()}
            </h4>
          </div>
          {isEmptyTable && (
            <Empty
              title="This page helps you identify frequently executed or high latency SQL statements."
              description="No SQL statements were executed since this page was last cleared."
              buttonHref="https://www.cockroachlabs.com/docs/stable/admin-ui-statements-page.html"
            />
          )}
          {(data.length > 0 || search && search.length > 0) && (
            <div className="cl-table-wrapper">
              <StatementsSortedTable
                className="statements-table"
                data={this.filteredStatementsData()}
                columns={makeStatementsColumns(statements, selectedApp, search, activateDiagnosticsRef)}
                sortSetting={this.state.sortSetting}
                onChangeSortSetting={this.changeSortSetting}
                renderNoResult={this.noStatementResult()}
                pagination={pagination}
              />
            </div>
          )}
        </section>
        <Pagination
          size="small"
          itemRender={this.renderPage as (page: number, type: "page" | "prev" | "next" | "jump-prev" | "jump-next") => React.ReactNode}
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={this.filteredStatementsData().length}
          onChange={this.onChangePage}
          hideOnSinglePage
        />
      </>
    );
  }
}

export default withRouter(StatementSortTable);
