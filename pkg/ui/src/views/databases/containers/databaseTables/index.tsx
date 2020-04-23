// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import { Link } from "react-router-dom";
import { refreshDatabaseDetails, refreshTableDetails, refreshTableStats } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { Bytes } from "src/util/format";
import { databaseDetails, DatabaseSummaryBase, DatabaseSummaryExplicitData, grants, tableInfos as selectTableInfos } from "src/views/databases/containers/databaseSummary";
import { TableInfo } from "src/views/databases/data/tableInfo";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
import "./databaseTables.styl";
import { DatabaseIcon } from "oss/src/components/icon/databaseIcon";
import Stack from "assets/stack.svg";
import { SummaryCard } from "oss/src/views/shared/components/summaryCard";
import { Row, Col } from "antd";

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

  noDatabaseResults = () => (
    <>
      <h3 className="table__no-results--title"><DatabaseIcon />This database has no tables.</h3>
    </>
  )

  render() {
    const { tableInfos, dbResponse, sortSetting } = this.props;
    const dbID = this.props.name;
    const loading = dbResponse ? !!dbResponse.inFlight : true;
    const numTables = tableInfos && tableInfos.length || 0;
    return (
      <div className="database-summary">
        <div className="database-summary-title">
          <h2 className="base-heading"><img src={Stack} alt="Stack" />{dbID}</h2>
        </div>
        <div className="l-columns">
          <div className="l-columns__left">
            <DatabaseTableListSortedTable
              data={tableInfos}
              sortSetting={sortSetting}
              onChangeSortSetting={(setting) => this.props.setSort(setting)}
              firstCellBordered
              columns={[
                {
                  title: "Table Name",
                  cell: (tableInfo) => {
                    return (
                      <div className="sort-table__unbounded-column">
                        <Link to={`/database/${dbID}/table/${tableInfo.name}`}><DatabaseIcon /> {tableInfo.name}</Link>
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
              ]}
              loading={loading}
              renderNoResult={loading ? undefined : this.noDatabaseResults()}
          />
          </div>
          <div className="l-columns__right">
            <SummaryCard>
              <Row>
                <Col span={24}>
                  <div className="summary--card__counting">
                    <h3 className="summary--card__counting--value">{Bytes(this.totalSize())}</h3>
                    <p className="summary--card__counting--label">Database Size</p>
                  </div>
                </Col>
                <Col span={24}>
                  <div className="summary--card__counting">
                    <h3 className="summary--card__counting--value">{numTables}</h3>
                    <p className="summary--card__counting--label">{(numTables === 1) ? "Table" : "Tables"}</p>
                  </div>
                </Col>
                <Col span={24}>
                  <div className="summary--card__counting">
                    <h3 className="summary--card__counting--value">{this.totalRangeCount()}</h3>
                    <p className="summary--card__counting--label">Total Range Count</p>
                  </div>
                </Col>
              </Row>
            </SummaryCard>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => ({ // RootState contains declaration for whole state
  tableInfos: selectTableInfos(state, ownProps.name),
  sortSetting: databaseTablesSortSetting.selector(state),
  dbResponse: databaseDetails(state)[ownProps.name],
  grants: grants(state, ownProps.name),
});

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
