// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Icon, Pagination, Tooltip } from "antd";
import _ from "lodash";
import moment from "moment";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps } from "react-router";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { refreshStatements } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { StatementsResponseMessage } from "src/util/api";
import { aggregateStatementStats, combineStatementStats, ExecutionStatistics, flattenStatementStats, StatementStatistics } from "src/util/appStats";
import { appAttr } from "src/util/constants";
import { TimestampToMoment } from "src/util/convert";
import { Pick } from "src/util/pick";
import { PrintTime } from "src/views/reports/containers/range/print";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { Search } from "../app/components/Search";
import "./statements.styl";
import { AggregateStatistics, makeStatementsColumns, StatementsSortedTable } from "./statementsTable";

type ICollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type RouteProps = RouteComponentProps<any, any>;

interface StatementsPageProps {
  statements: AggregateStatistics[];
  statementsError: Error | null;
  apps: string[];
  totalFingerprints: number;
  lastReset: string;
  refreshStatements: typeof refreshStatements;
}
type PaginationSettings = {
  pageSize: number;
  current: number;
};

interface StatementsPageState {
  sortSetting: SortSetting;
  pagination: PaginationSettings;
  search?: string;
}

export class StatementsPage extends React.Component<StatementsPageProps & RouteProps, StatementsPageState> {

  constructor(props: StatementsPageProps & RouteProps) {
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
    this.props.router.push(`/statements/${app.value}`);
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
          <Tooltip placement="bottom" title="Previous 5 pages">
            <div className="_pg-jump">
              <Icon type="left" />
              <span className="_jump-dots">•••</span>
            </div>
          </Tooltip>
        );
      case "jump-next":
        return (
          <Tooltip placement="bottom" title="Next 5 pages">
            <div className="_pg-jump">
              <Icon type="right" />
              <span className="_jump-dots">•••</span>
            </div>
          </Tooltip>
        );
      default:
        return originalElement;
    }
  }

  renderCounts = () => {
    const { pagination: { current, pageSize }, search } = this.state;
    const selectedApp = this.props.params[appAttr] || null;
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
    return `Last cleared ${moment.utc(lastReset).fromNow()}`;
  }

  renderStatements = () => {
    const { pagination, search } = this.state;
    const { statements } = this.props;
    const selectedApp = this.props.params[appAttr] || "";
    const appOptions = [{ value: "", label: "All" }];
    this.props.apps.forEach(app => appOptions.push({ value: app, label: app }));

    return (
      <React.Fragment>
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
        <section className="statements-table-container">
          <div className="statements-statistic">
            <h4 className="statement-count-title">
              {this.renderCounts()}
            </h4>
            <h4 className="last-cleared-title">
              {this.renderLastCleared()}
            </h4>
          </div>
          <StatementsSortedTable
            className="statements-table"
            data={this.getStatementsData()}
            columns={makeStatementsColumns(statements, selectedApp, search)}
            sortSetting={this.state.sortSetting}
            onChangeSortSetting={this.changeSortSetting}
            drawer
          />
        </section>
        <Pagination
          size="small"
          itemRender={this.renderPage as (page: number, type: "page" | "prev" | "next" | "jump-prev" | "jump-next") => React.ReactNode}
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={this.filteredStatementsData().length}
          onChange={this.onChangePage}
        />
      </React.Fragment>
    );
  }

  render() {
    return (
      <React.Fragment>
        <Helmet title={ this.props.params[appAttr] ? this.props.params[appAttr] + " App | Statements" : "Statements"} />

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
  (_state: StatementsState, props: { params: { [key: string]: string } }) => props,
  (state: CachedDataReducerState<StatementsResponseMessage>, props: RouteProps) => {
    if (!state.data) {
      return null;
    }

    let statements = flattenStatementStats(state.data.statements);
    if (props.params[appAttr]) {
      let criteria = props.params[appAttr];
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
const StatementsPageConnected = connect(
  (state: StatementsState, props: RouteProps) => ({
    statements: selectStatements(state, props),
    statementsError: state.cachedData.statements.lastError,
    apps: selectApps(state),
    totalFingerprints: selectTotalFingerprints(state),
    lastReset: selectLastReset(state),
  }),
  {
    refreshStatements,
  },
)(StatementsPage);

export default StatementsPageConnected;
