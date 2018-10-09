// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import { Link } from "react-router";

import { SummaryBar, SummaryHeadlineStat } from "src/views/shared/components/summaryBar";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";

import { AdminUIState } from "src/redux/state";
import { LocalSetting } from "src/redux/localsettings";
import {
    refreshDatabaseDetails, refreshTableDetails, refreshTableStats,
} from "src/redux/apiReducers";

import { Bytes } from "src/util/format";

import { TableInfo } from "src/views/databases/data/tableInfo";

import {
    DatabaseSummaryBase, DatabaseSummaryExplicitData, databaseDetails, tableInfos as selectTableInfos, grants,
} from "src/views/databases/containers/databaseSummary";

const databaseTablesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "databases/sort_setting/tables", (s) => s.localSettings,
);

class DatabaseTableListSortedTable extends SortedTable<TableInfo> {}

// DatabaseSummaryTables displays a summary section describing the tables
// contained in a single database.
class DatabaseSummaryTables extends DatabaseSummaryBase {
  totalSize() {
    const tableInfos = this.props.tableInfos;
    return _.sumBy(tableInfos, (ti) => ti.physicalSize);
  }

  totalRangeCount() {
    const tableInfos = this.props.tableInfos;
    return _.sumBy(tableInfos, (ti) => ti.rangeCount);
  }

  render() {
    const { tableInfos, sortSetting } = this.props;
    const dbID = this.props.name;

    const numTables = tableInfos && tableInfos.length || 0;

    return (
      <div className="database-summary">
        <div className="database-summary-title">
          <h2>{dbID}</h2>
        </div>
        <div className="l-columns">
          <div className="l-columns__left">
            <div className="database-summary-table sql-table">
              {
                (numTables === 0) ? "" :
                  <DatabaseTableListSortedTable
                    data={tableInfos}
                    sortSetting={sortSetting}
                    onChangeSortSetting={(setting) => this.props.setSort(setting)}
                    columns={[
                      {
                        title: "Table Name",
                        cell: (tableInfo) => {
                          return (
                            <div className="sort-table__unbounded-column">
                              <Link to={`/database/${dbID}/table/${tableInfo.name}`}>{tableInfo.name}</Link>
                            </div>
                          );
                        },
                        sort: (tableInfo) => tableInfo.name,
                        className: "expand-link", // don't pad the td element to allow the link to expand
                      },
                      {
                        title: "Size",
                        cell: (tableInfo) => Bytes(tableInfo.physicalSize),
                        sort: (tableInfo) => tableInfo.physicalSize,
                      },
                      {
                        title: "Ranges",
                        cell: (tableInfo) => tableInfo.rangeCount,
                        sort: (tableInfo) => tableInfo.rangeCount,
                      },
                      {
                        title: "# of Columns",
                        cell: (tableInfo) => tableInfo.numColumns,
                        sort: (tableInfo) => tableInfo.numColumns,
                      },
                      {
                        title: "# of Indices",
                        cell: (tableInfo) => tableInfo.numIndices,
                        sort: (tableInfo) => tableInfo.numIndices,
                      },
                    ]} />
              }
            </div>
          </div>
          <div className="l-columns__right">
            <SummaryBar>
              <SummaryHeadlineStat
                title="Database Size"
                tooltip="Approximate total disk size of this database across all replicas."
                value={this.totalSize()}
                format={Bytes} />
              <SummaryHeadlineStat
                title={(numTables === 1) ? "Table" : "Tables"}
                tooltip="The total number of tables in this database."
                value={numTables} />
              <SummaryHeadlineStat
                title="Total Range Count"
                tooltip="The total ranges across all tables in this database."
                value={this.totalRangeCount()} />
            </SummaryBar>
          </div>
        </div>
      </div>
    );
  }
}

// Connect the DatabaseSummaryTables class with redux store.
export default connect(
  (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => {
    return {
      tableInfos: selectTableInfos(state, ownProps.name),
      sortSetting: databaseTablesSortSetting.selector(state),
      dbResponse: databaseDetails(state)[ownProps.name] && databaseDetails(state)[ownProps.name].data,
      grants: grants(state, ownProps.name),
    };
  },
  {
    setSort: databaseTablesSortSetting.set,
    refreshDatabaseDetails,
    refreshTableDetails,
    refreshTableStats,
  },
)(DatabaseSummaryTables);
