// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Icon, Pagination } from "antd";
import { isNil, merge, forIn } from "lodash";
import moment from "moment";
import { DATE_FORMAT } from "src/util/format";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import { RouteComponentProps, withRouter } from "react-router-dom";
import classNames from "classnames/bind";
import { paginationPageCount } from "src/components/pagination/pagination";
import * as protos from "src/js/protos";
import { refreshStatementDiagnosticsRequests, refreshStatements } from "src/redux/apiReducers";
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
import { AggregateStatistics, makeStatementsColumns, StatementsSortedTable } from "./statementsTable";
import ActivateDiagnosticsModal, { ActivateDiagnosticsModalRef } from "src/views/statements/diagnostics/activateDiagnosticsModal";
import {
  selectLastDiagnosticsReportPerStatement,
} from "src/redux/statements/statementsSelectors";
import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";
import { getMatchParamByName } from "src/util/query";
import { trackPaginate, trackSearch } from "src/util/analytics";
import { ISortedTablePagination } from "../shared/components/sortedtable";
import { statementsTable } from "src/util/docs";
import styles from "./statementsPage.module.styl";
import sortableTableStyles from "src/views/shared/components/sortabletable/sortabletable.module.styl";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

type ICollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

interface OwnProps {
  statements: AggregateStatistics[];
  statementsError: Error | null;
  apps: string[];
  totalFingerprints: number;
  lastReset: string;
  refreshStatements: typeof refreshStatements;
  refreshStatementDiagnosticsRequests: typeof refreshStatementDiagnosticsRequests;
  dismissAlertMessage: () => void;
}

export interface StatementsPageState {
  sortSetting: SortSetting;
  search?: string;
  pagination: ISortedTablePagination;
}

export type StatementsPageProps = OwnProps & RouteComponentProps<any>;

export class StatementsPage extends React.Component<StatementsPageProps, StatementsPageState> {
  activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>;

  constructor(props: StatementsPageProps) {
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
      search: "",
    };

    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(defaultState, stateFromHistory);
    this.activateDiagnosticsRef = React.createRef();
  }

  getStateFromHistory = (): Partial<StatementsPageState> => {
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const sortKey = searchParams.get("sortKey") || undefined;
    const ascending = searchParams.get("ascending") || undefined;
    const searchQuery = searchParams.get("q") || undefined;

    return {
      sortSetting: {
        sortKey,
        ascending: Boolean(ascending),
      },
      search: searchQuery,
    };
  }

  syncHistory = (params: Record<string, string | undefined>) => {
    const { history } = this.props;
    const currentSearchParams = new URLSearchParams(history.location.search);
    // const nextSearchParams = new URLSearchParams(params);

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

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });

    this.syncHistory({
      "sortKey": ss.sortKey,
      "ascending": Boolean(ss.ascending).toString(),
    });
  }

  selectApp = (app: DropdownOption) => {
    const { history } = this.props;
    history.location.pathname = `/statements/${encodeURIComponent(app.value)}`;
    history.replace(history.location);
    this.resetPagination();
  }

  resetPagination = () => {
    this.setState((prevState) => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  }

  componentDidMount() {
    this.props.refreshStatements();
    this.props.refreshStatementDiagnosticsRequests();
  }

  componentDidUpdate = (__: StatementsPageProps, prevState: StatementsPageState) => {
    if (this.state.search && this.state.search !== prevState.search) {
      trackSearch(this.filteredStatementsData().length);
    }
    this.props.refreshStatements();
    this.props.refreshStatementDiagnosticsRequests();
  }

  componentWillUnmount() {
    this.props.dismissAlertMessage();
  }

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
    trackPaginate(current);
  }
  onSubmitSearchField = (search: string) => {
    this.setState({ search });
    this.resetPagination();
    this.syncHistory({
      "q": search,
    });
  }

  onClearSearchField = () => {
    this.setState({ search: "" });
    this.syncHistory({
      "q": undefined,
    });
  }

  filteredStatementsData = () => {
    const { search } = this.state;
    const { statements } = this.props;
    return statements.filter(statement => search.split(" ").every(val => statement.label.toLowerCase().includes(val.toLowerCase())));
  }

  renderPage = (_page: number, type: "page" | "prev" | "next" | "jump-prev" | "jump-next", originalElement: React.ReactNode) => {
    switch (type) {
      case "jump-prev":
        return (
          <div className={cx("_pg-jump")}>
            <Icon type="left" />
            <span className={cx("_jump-dots")}>•••</span>
          </div>
        );
      case "jump-next":
        return (
          <div className={cx("_pg-jump")}>
            <Icon type="right" />
            <span className={cx("_jump-dots")}>•••</span>
          </div>
        );
      default:
        return originalElement;
    }
  }

  renderLastCleared = () => {
    const { lastReset } = this.props;
    return `Last cleared ${moment.utc(lastReset).format(DATE_FORMAT)}`;
  }

  noStatementResult = () => (
    <>
      <h3 className={sortableTableCx("table__no-results--title")}>There are no SQL statements that match your search or filter since this page was last cleared.</h3>
      <p className={sortableTableCx("table__no-results--description")}>
        <span>Statements are cleared every hour by default, or according to your configuration.</span>
        <a href={statementsTable} target="_blank">Learn more</a>
      </p>
    </>
  )

  renderStatements = () => {
    const { pagination, search } = this.state;
    const { statements, match } = this.props;
    const appAttrValue = getMatchParamByName(match, appAttr);
    const selectedApp = appAttrValue || "";
    const appOptions = [{ value: "", label: "All" }];
    this.props.apps.forEach(app => appOptions.push({ value: app, label: app }));
    const data = this.filteredStatementsData();
    return (
      <div>
        <PageConfig>
          <PageConfigItem>
            <Search
              onSubmit={this.onSubmitSearchField as any}
              onClear={this.onClearSearchField}
              defaultValue={search}
            />
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown
              title="App"
              options={appOptions}
              selected={decodeURIComponent(selectedApp)}
              onChange={this.selectApp}
            />
          </PageConfigItem>
        </PageConfig>
        <section className={sortableTableCx("cl-table-container")}>
          <div className={cx("cl-table-statistic")}>
            <h4 className={cx("cl-count-title")}>
              {paginationPageCount({ ...pagination, total: this.filteredStatementsData().length }, "statements", match, appAttr, search)}
            </h4>
            <h4 className={cx("last-cleared-title")}>
              {this.renderLastCleared()}
            </h4>
          </div>
          <StatementsSortedTable
            className="statements-table"
            data={data}
            columns={
              makeStatementsColumns(
                statements,
                selectedApp,
                search,
                this.activateDiagnosticsRef,
              )
            }
            empty={data.length === 0 && search.length === 0}
            emptyProps={{
              title: "There are no statements since this page was last cleared.",
              description: "Statements help you identify frequently executed or high latency SQL statements. Statements are cleared every hour by default, or according to your configuration.",
              label: "Learn more",
              buttonHref: statementsTable,
            }}
            sortSetting={this.state.sortSetting}
            onChangeSortSetting={this.changeSortSetting}
            renderNoResult={this.noStatementResult()}
            pagination={pagination}
          />
        </section>
        <Pagination
          size="small"
          itemRender={this.renderPage as (page: number, type: "page" | "prev" | "next" | "jump-prev" | "jump-next") => React.ReactNode}
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={data.length}
          onChange={this.onChangePage}
          hideOnSinglePage
        />
      </div>
    );
  }

  render() {
    const { match } = this.props;
    const app = getMatchParamByName(match, appAttr);
    return (
      <React.Fragment>
        <Helmet title={app ? `${app} App | Statements` : "Statements"} />

        <section className={cx("section")}>
          <h1 className={cx("base-heading")}>Statements</h1>
        </section>

        <Loading
          loading={isNil(this.props.statements)}
          error={this.props.statementsError}
          render={this.renderStatements}
        />
        <ActivateDiagnosticsModal ref={this.activateDiagnosticsRef} />
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
  selectLastDiagnosticsReportPerStatement,
  (
    state: CachedDataReducerState<StatementsResponseMessage>,
    props: RouteComponentProps<any>,
    lastDiagnosticsReportPerStatement,
  ) => {
    if (!state.data) {
      return null;
    }
    let statements = flattenStatementStats(state.data.statements);
    const app = getMatchParamByName(props.match, appAttr);
    const isInternal = (statement: ExecutionStatistics) => statement.app.startsWith(state.data.internal_app_name_prefix);

    if (app) {
      let criteria = decodeURIComponent(app);
      let showInternal = false;
      if (criteria === "(unset)") {
        criteria = "";
      } else if (criteria === "(internal)") {
        showInternal = true;
      }

      statements = statements.filter(
        (statement: ExecutionStatistics) => (showInternal && isInternal(statement)) || statement.app === criteria,
      );
    } else {
      statements = statements.filter((statement: ExecutionStatistics) => !isInternal(statement));
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
        diagnosticsReport: lastDiagnosticsReportPerStatement[stmt.statement],
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
  (state: AdminUIState, props: RouteComponentProps) => ({
    statements: selectStatements(state, props),
    statementsError: state.cachedData.statements.lastError,
    apps: selectApps(state),
    totalFingerprints: selectTotalFingerprints(state),
    lastReset: selectLastReset(state),
  }),
  {
    refreshStatements,
    refreshStatementDiagnosticsRequests,
    dismissAlertMessage: () => createStatementDiagnosticsAlertLocalSetting.set({ show: false }),
  },
)(StatementsPage));

export default StatementsPageConnected;
