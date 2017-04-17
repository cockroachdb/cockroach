import _ from "lodash";
import * as React from "react";
import { connect } from "react-redux";
import { Link } from "react-router";

import { SummaryBar, SummaryHeadlineStat } from "../../components/summaryBar";
import { SortSetting } from "../../components/sortabletable";
import { SortedTable } from "../../components/sortedtable";

import { AdminUIState } from "../../redux/state";
import { LocalSetting } from "../../redux/localsettings";
import {
    refreshDatabaseDetails, refreshTableDetails, refreshTableStats,
} from "../../redux/apiReducers";

import { Bytes } from "../../util/format";

import { TableInfo } from "./data";

import {
    DatabaseSummaryBase, DatabaseSummaryExplicitData, databaseDetails, tableInfos, grants,
} from "./databaseSummary";

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
    let tableInfos = this.props.tableInfos;
    return _.sumBy(tableInfos, (ti) => ti.size);
  }

  totalRangeCount() {
    let tableInfos = this.props.tableInfos;
    return _.sumBy(tableInfos, (ti) => ti.rangeCount);
  }

  render() {
    let { tableInfos, sortSetting } = this.props;
    let dbID = this.props.name;

    let numTables = tableInfos && tableInfos.length || 0;

    return <div className="database-summary l-columns">
      <div className="database-summary-title">
        { dbID }
      </div>
      <div className="l-columns__left">
        <div className="database-summary-table sql-table">
        {
          (numTables === 0) ? "" :
          <DatabaseTableListSortedTable
              data={tableInfos}
              sortSetting={sortSetting}
              onChangeSortSetting={(setting) => this.props.setSort(setting) }
              columns={[
              {
                title: "Table Name",
                cell: (tableInfo) => {
                  return <Link to={`databases/database/${dbID}/table/${tableInfo.name}`}>{tableInfo.name}</Link>;
                },
                sort: (tableInfo) => tableInfo.name,
                className: "expand-link", // don't pad the td element to allow the link to expand
              },
              {
                title: "Size",
                cell: (tableInfo) => Bytes(tableInfo.size),
                sort: (tableInfo) => tableInfo.size,
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
              {
                title: "Schema Change",
                cell: (_tableInfo) => "",
              },
              ]}/>
        }
        </div>
      </div>
      <div className="l-columns__right">
        <SummaryBar>
          <SummaryHeadlineStat
            title="Database Size"
            tooltip="Total disk size of this database."
            value={ this.totalSize() }
            format={ Bytes }/>
          <SummaryHeadlineStat
            title={ (numTables === 1) ? "Table" : "Tables" }
            tooltip="The total number of tables in this database."
            value={ numTables }/>
          <SummaryHeadlineStat
            title="Total Range Count"
            tooltip="The total ranges across all tables in this database."
            value={ this.totalRangeCount() }/>
        </SummaryBar>
      </div>
    </div>;
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
