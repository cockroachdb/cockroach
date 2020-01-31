// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import tableIcon from "!!raw-loader!assets/tableIcon.svg";
import { PaginationComponent } from "oss/src/components/pagination/pagination";
import React from "react";
import { connect } from "react-redux";
import { Link } from "react-router-dom";
import { refreshDatabaseDetails, refreshTableDetails, refreshTableStats } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { Bytes } from "src/util/format";
import { trustIcon } from "src/util/trust";
import { databaseDetails, DatabaseSummaryBase, DatabaseSummaryExplicitData, grants, tableInfos as selectTableInfos } from "src/views/databases/containers/databaseSummary";
import { TableInfo } from "src/views/databases/data/tableInfo";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
import "./databaseTables.styl";

const databaseTablesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "databases/sort_setting/tables", (s) => s.localSettings,
);

class DatabaseTableListSortedTable extends SortedTable<TableInfo> {}

class DatabaseTableListEmpty extends React.Component {
  render() {
    return (
      <table className="sort-table">
        <thead>
          <tr className="sort-table__row sort-table__row--header">
            <th className="sort-table__cell sort-table__cell--sortable">
              Table Name
            </th>
            <th className="sort-table__cell sort-table__cell--sortable">
              Size
            </th>
            <th className="sort-table__cell sort-table__cell--sortable">
              Ranges
            </th>
            <th className="sort-table__cell sort-table__cell--sortable">
              # of Columns
            </th>
            <th className="sort-table__cell sort-table__cell--sortable">
              # of Indices
            </th>
          </tr>
        </thead>
        <tbody>
          <tr className="sort-table__row sort-table__row--body">
            <td className="sort-table__cell" colSpan={5}>
              <div className="empty-state">
                <div className="empty-state__line">
                  <span className="table-icon" dangerouslySetInnerHTML={trustIcon(tableIcon)} />
                  This database has no tables.
                </div>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    );
  }
}

// DatabaseSummaryTables displays a summary section describing the tables
// contained in a single database.
class DatabaseSummaryTables extends DatabaseSummaryBase {
  state = {
    pagination: {
      pageSize: 20,
      current: 1,
    },
  };

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current }});
  }

  getDatabaseSummaryData = (values: any) => {
    const { pagination: { current, pageSize } } = this.state;
    const currentDefault = current - 1;
    const start = (currentDefault * pageSize);
    const end = (currentDefault * pageSize + pageSize);
    const data = values.slice(start, end);
    return data;
  }

  render() {
    const { pagination } = this.state;
    const { tableInfos, sortSetting } = this.props;
    const dbID = this.props.name;
    const numTables = tableInfos && tableInfos.length || 0;
    return (
      <div className="database-summary">
        <div className="database-summary-title">
          <h2 className="base-heading">{dbID}</h2>
        </div>
        <div className="l-columns">
          <div className="l-columns__left">
            <div className="database-summary-table sql-table">
              {
                (numTables === 0) ? <DatabaseTableListEmpty /> :
                <React.Fragment>
                  <DatabaseTableListSortedTable
                    data={this.getDatabaseSummaryData(tableInfos)}
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
                  <PaginationComponent
                    pagination={{ ...pagination, total: numTables }}
                    onChange={this.onChangePage}
                  />
                </React.Fragment>
              }
            </div>
          </div>
          {/* <div className="l-columns__right">
            <SummaryBarContainer name={name} />
          </div> */}
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => {
  return ({ // RootState contains declaration for whole state
    tableInfos: selectTableInfos(state, ownProps.name),
    sortSetting: databaseTablesSortSetting.selector(state),
    dbResponse: databaseDetails(state)[ownProps.name] && databaseDetails(state)[ownProps.name].data,
    grants: grants(state, ownProps.name),
  });
};

const mapDispatchToProps = {
  setSort: databaseTablesSortSetting.set,
  refreshDatabaseDetails,
  refreshTableDetails,
  refreshTableStats,
};

// Connect the DatabaseSummaryTables class with redux store.
export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(DatabaseSummaryTables as any);
