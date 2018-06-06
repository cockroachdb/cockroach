import React from "react";
import Helmet from "react-helmet";

import Loader from "src/util/loader";
import { getDataFromServer } from "src/util/dataFromServer";
import { getQueries, QueriesResponseMessage } from "src/util/api";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { Duration } from "src/util/format";
import { FixLong } from "src/util/fixLong";
import * as protos from "ccl/src/js/protos";
import "./queries.styl";

type Query$Properties = protos.cockroach.server.serverpb.QueriesResponse.Query$Properties;

class QueriesLoader extends Loader<QueriesResponseMessage> {}

class QueriesSortedTable extends SortedTable<Query$Properties> {}

interface QueriesPageState {
  sortSetting: SortSetting;
}

class QueriesPage extends React.Component<{}, QueriesPageState> {

  constructor() {
    super({});
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

  render() {
    return (
      <section className="section" style={{ maxWidth: "none" }}>
        <Helmet>
          <title>Queries</title>
        </Helmet>

        <h1 style={{ marginBottom: 20 }}>Queries for Node {getDataFromServer().GatewayNodeID}</h1>

        <QueriesLoader load={getQueries}>
          {(data, handleRefresh) => {
            const columns: ColumnDescriptor<Query$Properties>[] = [
              {
                title: "Query",
                className: "queries-table__col-query-text",
                cell: (query) => query.query_anonymized,
              },
              {
                title: "Count",
                cell: (query) => FixLong(query.count).toInt(),
                sort: (query) => FixLong(query.count).toInt(),
              },
              {
                title: "Avg Rows",
                cell: (query) => Math.round(query.rows_avg),
                sort: (query) => query.rows_avg,
              },
              {
                title: "Avg Latency",
                cell: (query) => Duration(query.service_lat_avg * 1e9),
                sort: (query) => query.service_lat_avg,
              },
            ];

            return (
              <div className="queries-screen">
                <button onClick={handleRefresh} style={{ padding: 2, margin: 2 }}>Refresh</button>

                <span className="queries-screen__last-hour-note">
                  {data.queries.length} query fingerprints.
                  Query history is only maintained for the past hour.
                </span>

                <QueriesSortedTable
                  className="queries-table"
                  data={data.queries}
                  columns={columns}
                  sortSetting={this.state.sortSetting}
                  onChangeSortSetting={this.changeSortSetting}
                />
              </div>
            );
          }}
        </QueriesLoader>
      </section>
    );
  }

}

export default QueriesPage;
