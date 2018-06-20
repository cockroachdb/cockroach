import _ from "lodash";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { Link, RouteComponentProps } from "react-router";

import spinner from "assets/spinner.gif";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { FixLong } from "src/util/fixLong";
import Print from "src/views/reports/containers/range/print";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { refreshQueries } from "src/redux/apiReducers";
import { QueriesResponseMessage } from "src/util/api";
import { appAttr } from "src/util/constants";
import { Duration } from "src/util/format";
import { summarize, StatementSummary } from "src/util/sql/summarize";

import { countBarChart, rowsBarChart, latencyBarChart } from "./barCharts";

import * as protos from "src/js/protos";
import "./statements.styl";

type CollectedStatementStatistics$Properties = protos.cockroach.sql.CollectedStatementStatistics$Properties;
type RouteProps = RouteComponentProps<any, any>;

class StatementsSortedTable extends SortedTable<CollectedStatementStatistics$Properties> {}

interface StatementsPageProps {
  statements: CachedDataReducerState<QueriesResponseMessage>;
  refreshQueries: typeof refreshQueries;
}

interface StatementsPageState {
  sortSetting: SortSetting;
}

function StatementLink(props: { statement: string }) {
  const summary = summarize(props.statement);

  return (
    <Link to={ `/statement/${encodeURIComponent(props.statement)}` }>
      <div title={ props.statement }>{ shortStatement(summary, props.statement) }</div>
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

function calculateCumulativeTime(query: CollectedStatementStatistics$Properties) {
  const count = FixLong(query.stats.count).toInt();
  const latency = query.stats.service_lat.mean;

  return count * latency;
}

function makeStatementsColumns(statements: CollectedStatementStatistics$Properties[])
    : ColumnDescriptor<CollectedStatementStatistics$Properties>[] {
  const countBar = countBarChart(statements);
  const rowsBar = rowsBarChart(statements);
  const latencyBar = latencyBarChart(statements);

  return [
    {
      title: "Statement",
      className: "statements-table__col-query-text",
      cell: (query) => <StatementLink statement={ query.key.query } />,
      sort: (query) => query.key.query,
    },
    {
      title: "Time",
      cell: (query) => Duration(calculateCumulativeTime(query) * 1e9),
      sort: calculateCumulativeTime,
    },
    {
      title: "Count",
      cell: countBar,
      sort: (query) => FixLong(query.stats.count).toInt(),
    },
    {
      title: "Mean Rows",
      cell: rowsBar,
      sort: (query) => query.stats.num_rows.mean,
    },
    {
      title: "Mean Latency",
      cell: latencyBar,
      sort: (query) => query.stats.service_lat.mean,
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

  getStatements() {
    if (!this.props.params[appAttr]) {
      return this.props.statements.data.queries;
    }

    let criteria = this.props.params[appAttr];
    if (criteria === "(unset)") {
      criteria = "";
    }

    return this.props.statements.data.queries.filter(
      (statement: CollectedStatementStatistics$Properties) =>
        statement.key.app === criteria,
    );
  }

  getApps() {
    const apps = {};
    this.props.statements.data.queries.forEach(
      (statement: CollectedStatementStatistics$Properties) => {
        if (statement.key.app) {
          apps[statement.key.app] = true;
        }
      }
    );
    return Object.keys(apps);
  }

  renderStatements() {
    if (!this.props.statements.data) {
      // This should really be handled by a loader component.
      return null;
    }

    const { last_reset } = this.props.statements.data;
    const queries = this.getStatements();

    const selectedApp = this.props.params[appAttr] || "";
    const appOptions = [{ value: "", label: "All" }, { value: "(unset)", label: "(unset)"  }];
    this.getApps().forEach(app => appOptions.push({ value: app, label: app }));

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
          {queries.length}
          {selectedApp ? ` of ${this.props.statements.data.queries.length} ` : " "}
          statement fingerprints.
          Query history is cleared once an hour;
          last cleared {Print.Timestamp(last_reset)}.
        </div>

        <StatementsSortedTable
          className="statements-table"
          data={queries}
          columns={makeStatementsColumns(queries)}
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
          <title>Statements</title>
        </Helmet>

        <h1 style={{ marginBottom: 20 }}>Statements</h1>

        <Loading
          loading={_.isNil(this.props.statements.data)}
          className="loading-image loading-image__spinner"
          image={spinner}
        >
          {this.renderStatements()}
        </Loading>
      </section>
    );
  }

}

// tslint:disable-next-line:variable-name
const StatementsPageConnected = connect(
  (state: AdminUIState) => ({
    statements: state.cachedData.queries,
  }),
  {
    refreshQueries,
  },
)(StatementsPage);

export default StatementsPageConnected;
