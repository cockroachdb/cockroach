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
    DatabaseSummaryBase, DatabaseSummaryExplicitData, databaseDetails, tableInfos, grants,
} from "src/views/databases/containers/databaseSummary";

const databaseTablesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "databases/sort_setting/tables", (s) => s.localSettings,
);

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const DatabaseTableListSortedTable = SortedTable as new () => SortedTable<TableInfo>;

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
      tableInfos: tableInfos(state, ownProps.name),
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
