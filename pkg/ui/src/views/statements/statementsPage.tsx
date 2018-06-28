import _ from "lodash";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { Link, RouteComponentProps } from "react-router";
import { createSelector } from "reselect";

import spinner from "assets/spinner.gif";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { FixLong } from "src/util/fixLong";
import { PrintTime } from "src/views/reports/containers/range/print";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import { refreshQueries } from "src/redux/apiReducers";
import { QueriesResponseMessage } from "src/util/api";
import { aggregateStatementStats, flattenStatementStats, combineStatementStats, StatementStatistics, ExecutionStatistics } from "src/util/appStats";
import { appAttr } from "src/util/constants";
import { TimestampToMoment } from "src/util/convert";
import { Duration } from "src/util/format";
import { Pick } from "src/util/pick";
import { summarize, StatementSummary } from "src/util/sql/summarize";

import { countBarChart, rowsBarChart, latencyBarChart } from "./barCharts";

import * as protos from "src/js/protos";
import "./statements.styl";

type CollectedStatementStatistics$Properties = protos.cockroach.sql.CollectedStatementStatistics$Properties;
type RouteProps = RouteComponentProps<any, any>;

interface AggregateStatistics {
  statement: string;
  stats: StatementStatistics;
}

class StatementsSortedTable extends SortedTable<AggregateStatistics> {}

interface StatementsPageProps {
  valid: boolean;
  statements: AggregateStatistics[];
  apps: string[];
  totalFingerprints: number;
  lastReset: string;
  refreshQueries: typeof refreshQueries;
}

interface StatementsPageState {
  sortSetting: SortSetting;
}

function StatementLink(props: { statement: string, app: string }) {
  const summary = summarize(props.statement);
  const base = props.app ? `/statements/${props.app}` : "/statement";

  return (
    <Link to={ `${base}/${encodeURIComponent(props.statement)}` }>
      <div className="__tooltip">
        <ToolTipWrapper text={ <pre style={{ whiteSpace: "pre-wrap" }}>{ props.statement }</pre> }>
          <div className="last-cleared-tooltip__tooltip-hover-area">
            { shortStatement(summary, props.statement) }
          </div>
        </ToolTipWrapper>
      </div>
    </Link>
  );
}

function shortStatement(summary: StatementSummary, original: string) {
  switch (summary.statement) {
    case "update": return "UPDATE " + summary.table;
    case "insert": return "INSERT INTO " + summary.table;
    case "select": return "SELECT FROM " + summary.table;
    case "delete": return "DELETE FROM " + summary.table;
    case "create": return "CREATE TABLE " + summary.table;
    default: return original;
  }
}

function calculateCumulativeTime(stats: StatementStatistics) {
  const count = FixLong(stats.count).toInt();
  const latency = stats.service_lat.mean;

  return count * latency;
}

function makeStatementsColumns(statements: AggregateStatistics[], selectedApp: string)
    : ColumnDescriptor<AggregateStatistics>[] {
  const countBar = countBarChart(statements);
  const rowsBar = rowsBarChart(statements);
  const latencyBar = latencyBarChart(statements);

  return [
    {
      title: "Statement",
      className: "statements-table__col-query-text",
      cell: (stmt) => <StatementLink statement={ stmt.statement } app={ selectedApp } />,
      sort: (stmt) => stmt.statement,
    },
    {
      title: "Time",
      cell: (stmt) => Duration(calculateCumulativeTime(stmt.stats) * 1e9),
      sort: (stmt) => calculateCumulativeTime(stmt.stats),
    },
    {
      title: "Count",
      cell: countBar,
      sort: (stmt) => FixLong(stmt.stats.count).toInt(),
    },
    {
      title: "Mean Rows",
      cell: rowsBar,
      sort: (stmt) => stmt.stats.num_rows.mean,
    },
    {
      title: "Mean Latency",
      cell: latencyBar,
      sort: (stmt) => stmt.stats.service_lat.mean,
    },
  ];
}

class StatementsPage extends React.Component<StatementsPageProps & RouteProps, StatementsPageState> {

  constructor(props: StatementsPageProps & RouteProps) {
    super(props);
    this.state = {
      sortSetting: {
        sortKey: 1,
        ascending: false,
      },
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
    this.props.refreshQueries();
  }

  componentWillReceiveProps() {
    this.props.refreshQueries();
  }

  renderStatements() {
    if (!this.props.valid) {
      // This should really be handled by a loader component.
      return null;
    }

    const selectedApp = this.props.params[appAttr] || "";
    const appOptions = [{ value: "", label: "All" }];
    this.props.apps.forEach(app => appOptions.push({ value: app, label: app }));

    const lastClearedHelpText = (
      <React.Fragment>
        Statement history is cleared once an hour by default, which can be
        configured with the cluster setting{" "}
        <code><pre style={{ display: "inline-block" }}>diagnostics.reporting.interval</pre></code>.
      </React.Fragment>
    );

    return (
      <div className="statements">
        <PageConfig>
          <PageConfigItem>
            <Dropdown
              title="App"
              options={appOptions}
              selected={selectedApp}
              onChange={this.selectApp}
            />
          </PageConfigItem>
        </PageConfig>

        <div className="statements__last-hour-note" style={{ marginTop: 20 }}>
          {this.props.statements.length}
          {selectedApp ? ` of ${this.props.totalFingerprints} ` : " "}
          statement fingerprints.
          Last cleared {this.props.lastReset}.
          <div className="last-cleared-tooltip__tooltip">
            <ToolTipWrapper text={lastClearedHelpText}>
              <div className="last-cleared-tooltip__tooltip-hover-area">
                <div className="last-cleared-tooltip__info-icon">i</div>
              </div>
            </ToolTipWrapper>
          </div>
        </div>

        <StatementsSortedTable
          className="statements-table"
          data={this.props.statements}
          columns={makeStatementsColumns(this.props.statements, selectedApp)}
          sortSetting={this.state.sortSetting}
          onChangeSortSetting={this.changeSortSetting}
        />
      </div>
    );
  }

  render() {
    return (
      <section className="section" style={{ maxWidth: "none" }}>
        <Helmet>
          <title>
            { this.props.params[appAttr] ? this.props.params[appAttr] + " App | Statements" : "Statements"}
          </title>
        </Helmet>

        <h1 style={{ marginBottom: 20 }}>Statements</h1>

        <Loading
          loading={_.isNil(this.props.statements)}
          className="loading-image loading-image__spinner"
          image={spinner}
        >
          {this.renderStatements()}
        </Loading>
      </section>
    );
  }
}

type QueriesState = Pick<AdminUIState, "cachedData", "queries">;

// selectStatements returns the array of AggregateStatistics to show on the
// StatementsPage, based on if the appAttr route parameter is set.
export const selectStatements = createSelector(
  (state: QueriesState) => state.cachedData.queries,
  (_state: QueriesState, props: { params: { [key: string]: string } }) => props,
  (state: CachedDataReducerState<QueriesResponseMessage>, props: RouteProps) => {
    if (!state.data) {
      return null;
    }

    let statements = flattenStatementStats(state.data.queries);
    if (props.params[appAttr]) {
      let criteria = props.params[appAttr];
      if (criteria === "(unset)") {
        criteria = "";
      }

      statements = statements.filter(
        (statement: ExecutionStatistics) => statement.app === criteria,
      );
    }

    const statementsMap: { [statement: string]: StatementStatistics[] } = {};
    statements.forEach(stmt => {
      const matches = statementsMap[stmt.statement] || (statementsMap[stmt.statement] = []);
      matches.push(stmt.stats);
    });

    return Object.keys(statementsMap).map(stmt => {
      const stats = statementsMap[stmt];
      return {
        statement: stmt,
        stats: combineStatementStats(stats),
      };
    });
  },
);

// selectApps returns the array of all apps with statement statistics present
// in the data.
export const selectApps = createSelector(
  (state: QueriesState) => state.cachedData.queries,
  (state: CachedDataReducerState<QueriesResponseMessage>) => {
    if (!state.data) {
      return [];
    }

    let sawBlank = false;
    const apps: { [app: string]: boolean } = {};
    state.data.queries.forEach(
      (statement: CollectedStatementStatistics$Properties) => {
        if (statement.key.app) {
          apps[statement.key.app] = true;
        } else {
          sawBlank = true;
        }
      },
    );
    return (sawBlank ? ["(unset)"] : []).concat(Object.keys(apps));
  },
);

// selectTotalFingerprints returns the count of distinct statement fingerprints
// present in the data.
export const selectTotalFingerprints = createSelector(
  (state: QueriesState) => state.cachedData.queries,
  (state: CachedDataReducerState<QueriesResponseMessage>) => {
    if (!state.data) {
      return 0;
    }
    const aggregated = aggregateStatementStats(state.data.queries);
    return aggregated.length;
  },
);

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(
  (state: QueriesState) => state.cachedData.queries,
  (state: CachedDataReducerState<QueriesResponseMessage>) => {
    if (!state.data) {
      return "unknown";
    }

    return PrintTime(TimestampToMoment(state.data.last_reset));
  },
);

// tslint:disable-next-line:variable-name
const StatementsPageConnected = connect(
  (state: QueriesState, props: RouteProps) => ({
    statements: selectStatements(state, props),
    apps: selectApps(state),
    totalFingerprints: selectTotalFingerprints(state),
    lastReset: selectLastReset(state),
    valid: state.cachedData.queries.valid,
  }),
  {
    refreshQueries,
  },
)(StatementsPage);

export default StatementsPageConnected;
