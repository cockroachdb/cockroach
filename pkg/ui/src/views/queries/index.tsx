import d3 from "d3";
import _ from "lodash";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { Link } from "react-router";

import Loading from "src/views/shared/components/loading";
import spinner from "assets/spinner.gif";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { FixLong } from "src/util/fixLong";
import Print from "src/views/reports/containers/range/print";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { refreshQueries } from "src/redux/apiReducers";
import { QueriesResponseMessage } from "src/util/api";
import { summarize, StatementSummary } from "src/util/sql/summarize";

import { countBarChart, rowsBarChart, latencyBarChart } from "./barCharts";

import * as protos from "src/js/protos";
import "./queries.styl";

type CollectedStatementStatistics$Properties = protos.cockroach.sql.CollectedStatementStatistics$Properties;

class QueriesSortedTable extends SortedTable<CollectedStatementStatistics$Properties> {}

interface QueriesPageProps {
  queries: CachedDataReducerState<QueriesResponseMessage>;
  refreshQueries: typeof refreshQueries;
}

interface QueriesPageState {
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

function makeQueriesColumns(queries: CollectedStatementStatistics$Properties[])
    : ColumnDescriptor<CollectedStatementStatistics$Properties>[] {
  const countBar = countBarChart(queries);
  const rowsBar = rowsBarChart(queries);
  const latencyBar = latencyBarChart(queries);

  return [
    {
      title: "Query",
      className: "queries-table__col-query-text",
      cell: (query) => <StatementLink statement={ query.key.query } />,
      sort: (query) => query.key.query,
    },
    {
      title: "Count",
      cell: countBar,
      sort: (query) => FixLong(query.stats.count).toInt(),
    },
    {
      title: "Avg Rows",
      cell: rowsBar,
      sort: (query) => query.stats.num_rows.mean,
    },
    {
      title: "Avg Latency",
      cell: latencyBar,
      sort: (query) => query.stats.service_lat.mean,
    },
  ];
}

class QueriesPage extends React.Component<QueriesPageProps, QueriesPageState> {

  constructor(props: QueriesPageProps) {
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

  componentWillMount() {
    this.props.refreshQueries();
  }

  componentWillReceiveProps() {
    this.props.refreshQueries();
  }

  renderQueries() {
    if (!this.props.queries.data) {
      // This should really be handled by a loader component.
      return null;
    }
    const { queries, last_reset } = this.props.queries.data;

    return (
      <div className="queries-screen">
        <span className="queries-screen__last-hour-note">
          {queries.length} query fingerprints.
          Query history is only maintained for about an hour.
          History last cleared {Print.Timestamp(last_reset)}.
        </span>

        <QueriesSortedTable
          className="queries-table"
          data={queries}
          columns={makeQueriesColumns(queries)}
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
          <title>Queries</title>
        </Helmet>

        <h1 style={{ marginBottom: 20 }}>Queries</h1>

        <Loading
          loading={_.isNil(this.props.queries.data)}
          className="loading-image loading-image__spinner"
          image={spinner}
        >
          {this.renderQueries()}
        </Loading>
      </section>
    );
  }

}

// tslint:disable-next-line:variable-name
const QueriesPageConnected = connect(
  (state: AdminUIState) => ({
    queries: state.cachedData.queries,
  }),
  {
    refreshQueries,
  },
)(QueriesPage);

export default QueriesPageConnected;
