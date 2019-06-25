// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Helmet } from "react-helmet";
import { Link, RouterState } from "react-router";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { databaseNameAttr, tableNameAttr } from "src/util/constants";
import { Bytes } from "src/util/format";
import { AdminUIState } from "src/redux/state";
import { LocalSetting } from "src/redux/localsettings";
import { refreshTableDetails, refreshTableStats, generateTableID } from "src/redux/apiReducers";
import { SummaryBar, SummaryHeadlineStat } from "src/views/shared/components/summaryBar";

import { TableInfo } from "src/views/databases/data/tableInfo";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
import { SqlBox } from "src/views/shared/components/sql/box";

class GrantsSortedTable extends SortedTable<protos.cockroach.server.serverpb.TableDetailsResponse.IGrant> {}

const databaseTableGrantsSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "tableDetails/sort_setting/grants", (s) => s.localSettings,
);

/**
 * TableMainData are the data properties which should be passed to the TableMain
 * container.
 */
interface TableMainData {
  tableInfo: TableInfo;
  grantsSortSetting: SortSetting;
}

/**
 * TableMainActions are the action dispatchers which should be passed to the
 * TableMain container.
 */
interface TableMainActions {
  // Refresh the table data
  refreshTableDetails: typeof refreshTableDetails;
  refreshTableStats: typeof refreshTableStats;
  setSort: typeof databaseTableGrantsSortSetting.set;
}

/**
 * TableMainProps is the type of the props object that must be passed to
 * TableMain component.
 */
type TableMainProps = TableMainData & TableMainActions & RouterState;

/**
 * TableMain renders the main content of the databases page, which is primarily a
 * data table of all databases.
 */
class TableMain extends React.Component<TableMainProps, {}> {
  componentWillMount() {
    this.props.refreshTableDetails(new protos.cockroach.server.serverpb.TableDetailsRequest({
      database: this.props.params[databaseNameAttr],
      table: this.props.params[tableNameAttr],
    }));
    this.props.refreshTableStats(new protos.cockroach.server.serverpb.TableStatsRequest({
      database: this.props.params[databaseNameAttr],
      table: this.props.params[tableNameAttr],
    }));
  }

  render() {
    const { tableInfo, grantsSortSetting } = this.props;

    const title = this.props.params[databaseNameAttr] + "." + this.props.params[tableNameAttr];

    if (tableInfo) {
      return <div>
        <Helmet>
          <title>{`${title} Table | Databases`}</title>
        </Helmet>
        <section className="section">
          <section className="section parent-link">
            <Link to="/databases/tables">&lt; Back to Databases</Link>
          </section>
          <div className="database-summary-title">
            <h2>{ title }</h2>
          </div>
          <div className="content l-columns">
            <div className="l-columns__left">
              <SqlBox value={ tableInfo.createStatement } />
              <div className="sql-table">
                <GrantsSortedTable
                  data={tableInfo.grants}
                  sortSetting={grantsSortSetting}
                  onChangeSortSetting={(setting) => this.props.setSort(setting) }
                  columns={[
                    {
                      title: "User",
                      cell: (grants) => grants.user,
                      sort: (grants) => grants.user,
                    },
                    {
                      title: "Grants",
                      cell: (grants) => grants.privileges.join(", "),
                      sort: (grants) => grants.privileges.join(", "),
                    },
                  ]}/>
              </div>
            </div>
            <div className="l-columns__right">
              <SummaryBar>
                <SummaryHeadlineStat
                  title="Size"
                  tooltip="Approximate total disk size of this table across all replicas."
                  value={ tableInfo.physicalSize }
                  format={ Bytes }/>
                <SummaryHeadlineStat
                  title="Ranges"
                  tooltip="The total number of ranges in this table."
                  value={ tableInfo.rangeCount }/>
              </SummaryBar>
            </div>
          </div>
        </section>
      </div>;
    }
    return <div>No results.</div>;
  }
}

/******************************
 *         SELECTORS
 */

function selectTableInfo(state: AdminUIState, props: RouterState): TableInfo {
  const db = props.params[databaseNameAttr];
  const table = props.params[tableNameAttr];
  const details = state.cachedData.tableDetails[generateTableID(db, table)];
  const stats = state.cachedData.tableStats[generateTableID(db, table)];
  return new TableInfo(table, details && details.data, stats && stats.data);
}

// Connect the TableMain class with our redux store.
const tableMainConnected = connect(
  (state: AdminUIState, ownProps: RouterState) => {
    return {
      tableInfo: selectTableInfo(state, ownProps),
      grantsSortSetting: databaseTableGrantsSortSetting.selector(state),
    };
  },
  {
    setSort: databaseTableGrantsSortSetting.set,
    refreshTableDetails,
    refreshTableStats,
  },
)(TableMain);

export default tableMainConnected;
