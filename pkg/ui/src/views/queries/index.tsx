import _ from "lodash";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";

import Loading from "src/views/shared/components/loading";
import spinner from "assets/spinner.gif";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { Duration } from "src/util/format";
import { FixLong } from "src/util/fixLong";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { refreshQueries } from "src/redux/apiReducers";
import { QueriesResponseMessage } from "src/util/api";

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

const QUERIES_COLUMNS: ColumnDescriptor<CollectedStatementStatistics$Properties>[] = [
  {
    title: "Query",
    className: "queries-table__col-query-text",
    cell: (query) => query.key.query,
    sort: (query) => query.key.query,
  },
  {
    title: "Count",
    cell: (query) => FixLong(query.stats.count).toInt(),
    sort: (query) => FixLong(query.stats.count).toInt(),
  },
  {
    title: "Avg Rows",
    cell: (query) => Math.round(query.stats.num_rows.mean),
    sort: (query) => query.stats.num_rows.mean,
  },
  {
    title: "Avg Latency",
    cell: (query) => Duration(query.stats.service_lat.mean * 1e9),
    sort: (query) => query.stats.service_lat.mean,
  },
];

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
    const queries = this.props.queries.data.queries;

    return (
      <div className="queries-screen">
        <span className="queries-screen__last-hour-note">
          {queries.length} query fingerprints.
          Query history is only maintained for the past hour.
        </span>

        <QueriesSortedTable
          className="queries-table"
          data={queries}
          columns={QUERIES_COLUMNS}
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
