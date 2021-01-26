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
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import {
  databaseDetails,
  DatabaseSummaryExplicitData,
  grants as selectGrants,
  DatabaseSummaryBase,
} from "src/views/databases/containers/databaseSummary";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
import { SummaryCard } from "src/views/shared/components/summaryCard";
import { DatabaseIcon } from "src/components/icon/databaseIcon";
import Stack from "assets/stack.svg";
import { SummaryHeadlineStat } from "src/views/shared/components/summaryBar";
import TitleWithIcon from "../../components/titleWithIcon/titleWithIcon";
import { refreshDatabaseDetails } from "src/redux/apiReducers";
import { privileges } from "src/util/docs";

class DatabaseGrantsSortedTable extends SortedTable<protos.cockroach.server.serverpb.DatabaseDetailsResponse.Grant> {}

const grantsSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "databases/sort_setting/grants",
  (s) => s.localSettings,
);

// DatabaseSummaryGrants displays a summary section describing the grants
// active on a single database.
export class DatabaseSummaryGrants extends DatabaseSummaryBase {
  totalUsers() {
    const grants = this.props.grants;
    return grants && grants.length;
  }

  noDatabaseResults = () => (
    <>
      <h3 className="table__no-results--title">
        <DatabaseIcon />
        No users have been granted access to this database.
      </h3>
      <p className="table__no-results--description">
        <a href={privileges} target="_blank" rel="noreferrer">
          Read more about privileges.
        </a>
      </p>
    </>
  );

  render() {
    const { grants, sortSetting, dbResponse } = this.props;
    const dbID = this.props.name;
    const loading = dbResponse ? !!dbResponse.inFlight : true;

    return (
      <div className="database-summary">
        <div className="database-summary-title">
          <TitleWithIcon src={Stack} title={dbID} />
        </div>
        <div className="l-columns">
          <div className="l-columns__left">
            <DatabaseGrantsSortedTable
              data={
                grants as protos.cockroach.server.serverpb.DatabaseDetailsResponse.Grant[]
              }
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
              loading={loading}
              renderNoResult={loading ? undefined : this.noDatabaseResults()}
            />
          </div>
          <div className="l-columns__right">
            <SummaryCard>
              <SummaryHeadlineStat
                title="Total Users"
                tooltip="Total users that have been granted permissions on this database."
                value={this.totalUsers()}
              />
            </SummaryCard>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (
  state: AdminUIState,
  ownProps: DatabaseSummaryExplicitData,
) => ({
  // RootState contains declaration for whole state
  // tableInfos: tableInfos(state, ownProps.name),
  sortSetting: grantsSortSetting.selector(state),
  dbResponse: databaseDetails(state)[ownProps.name],
  grants: selectGrants(state, ownProps.name),
});

const mapDispatchToProps = {
  setSort: grantsSortSetting.set,
  refreshDatabaseDetails: refreshDatabaseDetails,
};

// Connect the DatabaseSummaryGrants class with our redux store.
export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(DatabaseSummaryGrants as any);
