// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Icon } from "antd";
import _ from "lodash";
import moment from "moment";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import { PaginationComponent, PaginationSettings } from "src/components/pagination/pagination";
import * as protos from "src/js/protos";
import { refreshStatements } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { StatementsResponseMessage } from "src/util/api";
import { aggregateStatementStats, combineStatementStats, ExecutionStatistics, flattenStatementStats, StatementStatistics } from "src/util/appStats";
import { appAttr } from "src/util/constants";
import { TimestampToMoment } from "src/util/convert";
import { DATE_FORMAT } from "src/util/format";
import { Pick } from "src/util/pick";
import { getMatchParamByName } from "src/util/query";
import { PrintTime } from "src/views/reports/containers/range/print";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import Empty from "../app/components/empty";
import { Search } from "../app/components/Search";
import "./statements.styl";
import { AggregateStatistics, makeStatementsColumns, StatementsSortedTable } from "./statementsTable";

type ICollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

interface StatementsPageProps {
  statements: AggregateStatistics[];
  statementsError: Error | null;
  apps: string[];
  totalFingerprints: number;
  lastReset: string;
  refreshStatements: typeof refreshStatements;
}

interface StatementsPageState {
  sortSetting: SortSetting;
  pagination: PaginationSettings;
  search?: string;
}

export class StatementsPage extends React.Component<StatementsPageProps & RouteComponentProps<any>, StatementsPageState> {

  constructor(props: StatementsPageProps & RouteComponentProps<any>) {
    super(props);
    this.state = {
      sortSetting: {
        sortKey: 6,  // Latency
        ascending: false,
      },
      pagination: {
        pageSize: 20,
        current: 1,
      },
      search: "",
    };
  }

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  }

  selectApp = (app: DropdownOption) => {
    this.props.history.push(`/statements/${app.value}`);
  }

  componentWillMount() {
    this.props.refreshStatements();
  }

  componentWillReceiveProps() {
    this.props.refreshStatements();
  }

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current }});
  }

  getStatementsData = () => {
    const { pagination: { current, pageSize } } = this.state;
    const currentDefault = current - 1;
    const start = (currentDefault * pageSize);
    const end = (currentDefault * pageSize + pageSize);
    const data = this.filteredStatementsData().slice(start, end);
    return data;
  }

  onSubmitSearchField = (search: string) => this.setState({ pagination: { ...this.state.pagination, current: 1 }, search });

  onClearSearchField = () => this.setState({ search: "" });

  filteredStatementsData = () => {
    const { search } = this.state;
    const { statements } = this.props;
    return statements.filter(statement => search.split(" ").every(val => statement.label.toLowerCase().includes(val.toLowerCase())));
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
    const { pagination: { current, pageSize }, search } = this.state;
    const { match } = this.props;
    const appAttrValue = getMatchParamByName(match, appAttr);
    const selectedApp = appAttrValue || "";
    const total = this.filteredStatementsData().length;
    const pageCount = current * pageSize > total ? total : current * pageSize;
    const count = total > 10 ? pageCount : current * total;
    if (search.length > 0) {
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

  renderStatements = () => {
    const { pagination, search } = this.state;
    const { statements, match } = this.props;
    const appAttrValue = getMatchParamByName(match, appAttr);
    const selectedApp = appAttrValue || "";
    const appOptions = [{ value: "", label: "All" }];
    this.props.apps.forEach(app => appOptions.push({ value: app, label: app }));
    const data = this.getStatementsData();
    return (
      <div className="section">
        <PageConfig>
          <PageConfigItem>
            <Search
              onSubmit={this.onSubmitSearchField as any}
              onClear={this.onClearSearchField}
            />
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown
              title="App"
              options={appOptions}
              selected={selectedApp}
              onChange={this.selectApp}
            />
          </PageConfigItem>
        </PageConfig>
        <section className="cl-table-container">
          <div className="cl-table-statistic">
            <h4 className="cl-count-title">
              {this.renderCounts()}
            </h4>
            <h4 className="last-cleared-title">
              {this.renderLastCleared()}
            </h4>
          </div>
          {data.length === 0 && search.length === 0 && (
            <Empty
              title="This page helps you identify frequently executed or high latency SQL statements."
              description="No SQL statements were executed since this page was last cleared."
              buttonHref="https://www.cockroachlabs.com/docs/stable/admin-ui-statements-page.html"
            />
          )}
          {(data.length > 0 || search.length > 0) && (
            <div className="cl-table-wrapper">
              <StatementsSortedTable
                className="statements-table"
                data={data}
                columns={makeStatementsColumns(statements, selectedApp, search)}
                sortSetting={this.state.sortSetting}
                onChangeSortSetting={this.changeSortSetting}
              />
            </div>
          )}
        </section>
        <PaginationComponent
          pagination={{ ...pagination, total: this.filteredStatementsData().length }}
          onChange={this.onChangePage}
          hideOnSinglePage={data.length === 0}
        />
      </div>
    );
  }

  render() {
    const { match } = this.props;
    const app = getMatchParamByName(match, appAttr);
    return (
      <React.Fragment>
        <Helmet title={ app ? `${app} App | Statements` : "Statements"} />

        <section className="section">
          <h1 className="base-heading">Statements</h1>
        </section>

        <Loading
          loading={_.isNil(this.props.statements)}
          error={this.props.statementsError}
          render={this.renderStatements}
        />
      </React.Fragment>
    );
  }
}

type StatementsState = Pick<AdminUIState, "cachedData", "statements">;

interface StatementsSummaryData {
  statement: string;
  implicitTxn: boolean;
  stats: StatementStatistics[];
}

function keyByStatementAndImplicitTxn(stmt: ExecutionStatistics): string {
  return stmt.statement + stmt.implicit_txn;
}

// selectStatements returns the array of AggregateStatistics to show on the
// StatementsPage, based on if the appAttr route parameter is set.
export const selectStatements = createSelector(
  (state: StatementsState) => state.cachedData.statements,
  (_state: StatementsState, props: RouteComponentProps) => props,
  (state: CachedDataReducerState<StatementsResponseMessage>, props: RouteComponentProps<any>) => {
    if (!state.data) {
      return null;
    }
    let statements = flattenStatementStats(state.data.statements);
    const app = getMatchParamByName(props.match, appAttr);

    if (app) {
      let criteria = app;
      let showInternal = false;
      if (criteria === "(unset)") {
        criteria = "";
      } else if (criteria === "(internal)") {
        showInternal = true;
      }

      statements = statements.filter(
        (statement: ExecutionStatistics) => (showInternal && statement.app.startsWith(state.data.internal_app_name_prefix)) || statement.app === criteria,
      );
    }

    const statsByStatementAndImplicitTxn: { [statement: string]: StatementsSummaryData } = {};
    statements.forEach(stmt => {
      const key = keyByStatementAndImplicitTxn(stmt);
      if (!(key in statsByStatementAndImplicitTxn)) {
        statsByStatementAndImplicitTxn[key] = {
          statement: stmt.statement,
          implicitTxn: stmt.implicit_txn,
          stats: [],
        };
      }
      statsByStatementAndImplicitTxn[key].stats.push(stmt.stats);
    });

    return Object.keys(statsByStatementAndImplicitTxn).map(key => {
      const stmt = statsByStatementAndImplicitTxn[key];
      return {
        label: stmt.statement,
        implicitTxn: stmt.implicitTxn,
        stats: combineStatementStats(stmt.stats),
      };
    });
  },
);

// selectApps returns the array of all apps with statement statistics present
// in the data.
export const selectApps = createSelector(
  (state: StatementsState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return [];
    }

    let sawBlank = false;
    let sawInternal = false;
    const apps: { [app: string]: boolean } = {};
    state.data.statements.forEach(
      (statement: ICollectedStatementStatistics) => {
        if (state.data.internal_app_name_prefix && statement.key.key_data.app.startsWith(state.data.internal_app_name_prefix)) {
          sawInternal = true;
        } else if (statement.key.key_data.app) {
          apps[statement.key.key_data.app] = true;
        } else {
          sawBlank = true;
        }
      },
    );
    return [].concat(sawInternal ? ["(internal)"] : []).concat(sawBlank ? ["(unset)"] : []).concat(Object.keys(apps));
  },
);

// selectTotalFingerprints returns the count of distinct statement fingerprints
// present in the data.
export const selectTotalFingerprints = createSelector(
  (state: StatementsState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return 0;
    }
    const aggregated = aggregateStatementStats(state.data.statements);
    return aggregated.length;
  },
);

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(
  (state: StatementsState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return "unknown";
    }

    return PrintTime(TimestampToMoment(state.data.last_reset));
  },
);

// tslint:disable-next-line:variable-name
const StatementsPageConnected = withRouter(connect(
  (state: StatementsState, props: RouteComponentProps) => ({
    statements: selectStatements(state, props),
    statementsError: state.cachedData.statements.lastError,
    apps: selectApps(state),
    totalFingerprints: selectTotalFingerprints(state),
    lastReset: selectLastReset(state),
  }),
  {
    refreshStatements,
  },
)(StatementsPage));

export default StatementsPageConnected;
