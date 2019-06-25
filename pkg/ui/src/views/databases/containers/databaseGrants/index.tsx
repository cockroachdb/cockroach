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
import { connect } from "react-redux";

import * as protos from  "src/js/protos";

import { SummaryBar, SummaryHeadlineStat } from "src/views/shared/components/summaryBar";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";

import { AdminUIState } from "src/redux/state";
import { LocalSetting } from "src/redux/localsettings";
import {
    refreshDatabaseDetails, refreshTableDetails, refreshTableStats,
} from "src/redux/apiReducers";

import {
    DatabaseSummaryBase, DatabaseSummaryExplicitData, databaseDetails, tableInfos, grants as selectGrants,
} from "src/views/databases/containers/databaseSummary";

class DatabaseGrantsSortedTable extends SortedTable<protos.cockroach.server.serverpb.DatabaseDetailsResponse.Grant> {}

const grantsSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "databases/sort_setting/grants", (s) => s.localSettings,
);

// DatabaseSummaryGrants displays a summary section describing the grants
// active on a single database.
class DatabaseSummaryGrants extends DatabaseSummaryBase {
  totalUsers() {
    const grants = this.props.grants;
    return grants && grants.length;
  }

  render() {
    const { grants, sortSetting } = this.props;
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
                  <DatabaseGrantsSortedTable
                    data={grants}
                    sortSetting={sortSetting}
                    onChangeSortSetting={(setting) => this.props.setSort(setting)}
                    columns={[
                      {
                        title: "User",
                        cell: (grant) => grant.user,
                        sort: (grant) => grant.user,
                      },
                      {
                        title: "Grants",
                        cell: (grant) => grant.privileges.join(", "),
                      },
                    ]} />
              }
            </div>
          </div>
          <div className="l-columns__right">
            <SummaryBar>
              <SummaryHeadlineStat
                title="Total Users"
                tooltip="Total users that have been granted permissions on this table."
                value={this.totalUsers()} />
            </SummaryBar>
          </div>
        </div>
      </div>
    );
  }
}

// Connect the DatabaseSummaryGrants class with our redux store.
export default connect(
  (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => {
    return {
      tableInfos: tableInfos(state, ownProps.name),
      sortSetting: grantsSortSetting.selector(state),
      dbResponse: databaseDetails(state)[ownProps.name] && databaseDetails(state)[ownProps.name].data,
      grants: selectGrants(state, ownProps.name),
    };
  },
  {
    setSort: grantsSortSetting.set,
    refreshDatabaseDetails,
    refreshTableDetails,
    refreshTableStats,
  },
)(DatabaseSummaryGrants);
