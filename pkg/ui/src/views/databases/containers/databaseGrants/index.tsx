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
import * as protos from "src/js/protos";
import { refreshDatabaseDetails, refreshTableDetails, refreshTableStats } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { databaseDetails, DatabaseSummaryExplicitData, grants as selectGrants, tableInfos, DatabaseSummaryBase } from "src/views/databases/containers/databaseSummary";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
import { SummaryCard } from "src/views/shared/components/summaryCard";
import { DatabaseIcon } from "src/components/icon/databaseIcon";
import Stack from "assets/stack.svg";
import { SummaryHeadlineStat } from "src/views/shared/components/summaryBar";
import TitleWithIcon from "../../components/titleWithIcon/titleWithIcon";

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

  noDatabaseResults = () => (
    <>
      <h3 className="table__no-results--title"><DatabaseIcon />This grands has no users.</h3>
      <p className="table__no-results--description">
        <a href="https://www.cockroachlabs.com/docs/stable/admin-ui-databases-page.html" target="_blank">Learn more</a>
      </p>
    </>
  )

  render() {
    const { grants, sortSetting } = this.props;
    const dbID = this.props.name;

    return (
      <div className="database-summary">
        <div className="database-summary-title">
          <TitleWithIcon src={Stack} title={dbID}/>
        </div>
        <div className="l-columns">
          <div className="l-columns__left">
            <DatabaseGrantsSortedTable
              data={grants as protos.cockroach.server.serverpb.DatabaseDetailsResponse.Grant[]}
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
              ]}
              renderNoResult={this.noDatabaseResults()}
            />
          </div>
          <div className="l-columns__right">
            <SummaryCard>
                <SummaryHeadlineStat
                  title="Total Users"
                  tooltip="Total users that have been granted permissions on this table."
                  value={this.totalUsers()}
                />
            </SummaryCard>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => ({ // RootState contains declaration for whole state
  tableInfos: tableInfos(state, ownProps.name),
  sortSetting: grantsSortSetting.selector(state),
  dbResponse: databaseDetails(state)[ownProps.name] && databaseDetails(state)[ownProps.name].data,
  grants: selectGrants(state, ownProps.name),
});

const mapDispatchToProps = {
  setSort: grantsSortSetting.set,
  refreshDatabaseDetails,
  refreshTableDetails,
  refreshTableStats,
};

// Connect the DatabaseSummaryGrants class with our redux store.
export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(DatabaseSummaryGrants as any);
