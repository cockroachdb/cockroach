import * as React from "react";
import { connect } from "react-redux";

import * as protos from  "../../js/protos";

import { SummaryBar, SummaryHeadlineStat } from "../../components/summaryBar";
import { SortSetting } from "../../components/sortabletable";
import { SortedTable } from "../../components/sortedtable";

import { AdminUIState } from "../../redux/state";
import { LocalSetting } from "../../redux/localsettings";
import {
    refreshDatabaseDetails, refreshTableDetails, refreshTableStats,
} from "../../redux/apiReducers";

import {
    DatabaseSummaryBase, DatabaseSummaryExplicitData, databaseDetails, tableInfos, grants,
} from "./databaseSummary";

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
export const DatabaseGrantsSortedTable = SortedTable as new () => SortedTable<protos.cockroach.server.serverpb.DatabaseDetailsResponse.Grant>;

const grantsSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "databases/sort_setting/grants", (s) => s.localSettings,
);

// DatabaseSummaryGrants displays a summary section describing the grants
// active on a single database.
class DatabaseSummaryGrants extends DatabaseSummaryBase {
  totalUsers() {
    let grants = this.props.grants;
    return grants && grants.length;
  }

  render() {
    let { grants, sortSetting } = this.props;
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
          <DatabaseGrantsSortedTable
              data={grants}
              sortSetting={sortSetting}
              onChangeSortSetting={(setting) => this.props.setSort(setting) }
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
              ]}/>
        }
        </div>
      </div>
      <div className="l-columns__right">
        <SummaryBar>
          <SummaryHeadlineStat
            title="Total Users"
            tooltip="Total users that have been granted permissions on this table."
            value={ this.totalUsers() }/>
        </SummaryBar>
      </div>
    </div>;
  }
}

// Connect the DatabaseSummaryGrants class with our redux store.
export default connect(
  (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => {
    return {
      tableInfos: tableInfos(state, ownProps.name),
      sortSetting: grantsSortSetting.selector(state),
      dbResponse: databaseDetails(state)[ownProps.name] && databaseDetails(state)[ownProps.name].data,
      grants: grants(state, ownProps.name),
    };
  },
  {
    setSort: grantsSortSetting.set,
    refreshDatabaseDetails,
    refreshTableDetails,
    refreshTableStats,
  },
)(DatabaseSummaryGrants);
