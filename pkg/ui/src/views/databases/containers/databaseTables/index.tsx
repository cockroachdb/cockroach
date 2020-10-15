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
import {
  databaseDetails,
  DatabaseSummaryBase,
  DatabaseSummaryExplicitData,
  grants,
  tableInfos as selectTableInfos,
} from "src/views/databases/containers/databaseSummary";
import { TableInfo } from "src/views/databases/data/tableInfo";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
import "./databaseTables.styl";
import { DatabaseIcon } from "src/components/icon/databaseIcon";
import Stack from "assets/stack.svg";
import { SummaryCard } from "src/views/shared/components/summaryCard";
import { SummaryHeadlineStat } from "src/views/shared/components/summaryBar";
import TitleWithIcon from "../../components/titleWithIcon/titleWithIcon";
import { ReplicatedSizeTooltip } from "src/views/databases/containers/databases/tooltips";
import { Anchor } from "src/components";
import { Icon } from "antd";

const databaseTablesSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "databases/sort_setting/tables", (s) => s.localSettings,
);

class DatabaseTableListSortedTable extends SortedTable<TableInfo> {}

// DatabaseSummaryTables displays a summary section describing the tables
// contained in a single database.
export class DatabaseSummaryTables extends DatabaseSummaryBase {
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

  handleOnTitleClick = () => {
    const nextStateIsExpanded = !this.state.isExpanded;
    this.setState({ isExpanded: nextStateIsExpanded });
    if (nextStateIsExpanded) {
      this.loadTableDetails(this.props);
    }
  }

  render() {
    const { tableInfos, dbResponse, sortSetting } = this.props;
    const { isExpanded } = this.state;
    const dbID = this.props.name;
    const loading = dbResponse ? !!dbResponse.inFlight : true;
    const numTables = tableInfos && tableInfos.length || 0;
    const isLoadingTableInfos = tableInfos?.some(ti => !ti.id);
    return (
      <div className={`database-summary database-summary--${isExpanded ? "expanded" : "collapsed"}`}>
        <div className="database-summary-title">
          <Anchor
            onClick={this.handleOnTitleClick}
            className="database-summary__title-anchor"
          >
            <TitleWithIcon src={Stack} title={dbID}/>
            <Icon
              className="database-summary__title-anchor--toggle-icon"
              type={isExpanded ? "caret-down" : "caret-right"}
            />
          </Anchor>
        </div>
        <div className={`l-columns database-summary__body--${isExpanded ? "expanded" : "collapsed"}`} >
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
                      <div className="sort-table__unbounded-column table-name">
                        <Link to={`/database/${dbID}/table/${tableInfo.name}`}><DatabaseIcon /> {tableInfo.name}</Link>
                      </div>
                    );
                  },
                  sort: (tableInfo) => tableInfo.name,
                  className: "expand-link", // don't pad the td element to allow the link to expand
                },
                {
                  title: <ReplicatedSizeTooltip tableName={dbID}>{"Replicated Size"}</ReplicatedSizeTooltip>,
                  cell: (tableInfo) => _.isUndefined(tableInfo.physicalSize) ? "" : Bytes(tableInfo.physicalSize),
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
              loading={loading || isLoadingTableInfos}
              renderNoResult={loading ? undefined : this.noDatabaseResults()}
          />
          </div>
          <div className="l-columns__right">
            <SummaryCard>
                <SummaryHeadlineStat
                  title="Database Size"
                  tooltip="Approximate total disk size of this database across all table replicas."
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
