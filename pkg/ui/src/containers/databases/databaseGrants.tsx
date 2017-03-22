import * as React from "react";
import { connect } from "react-redux";

import { SummaryBar, SummaryHeadlineStat } from "../../components/summaryBar";
import { SortSetting } from "../../components/sortabletable";
import { SortedTable } from "../../components/sortedtable";

import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import {
    refreshDatabaseDetails, refreshTableDetails, refreshTableStats,
} from "../../redux/apiReducers";

import {
    DatabaseSummaryBase, DatabaseSummaryExplicitData, databaseDetails, tableInfos, grants,
} from "./databaseSummary";

type Grant = Proto2TypeScript.cockroach.server.serverpb.DatabaseDetailsResponse.Grant;

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
export const DatabaseGrantsSortedTable = SortedTable as new () => SortedTable<Grant>;

// Constants used to store per-page sort settings in the redux UI store.
const UI_DATABASE_GRANTS_SORT_SETTING_KEY = "databases/sort_setting/grants";

// DatabaseSummaryGrants displays a summary section describing the grants
// active on a single database.
class DatabaseSummaryGrants extends DatabaseSummaryBase {
  // Callback when the user elects to change the table table sort setting.
  changeTableSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASE_GRANTS_SORT_SETTING_KEY, setting);
  }

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
              onChangeSortSetting={(setting) => this.changeTableSortSetting(setting) }
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

// Base selectors to extract data from redux state.
let grantsSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASE_GRANTS_SORT_SETTING_KEY] || {};

// Connect the DatabaseSummaryGrants class with our redux store.
export default connect(
  (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => {
    return {
      tableInfos: tableInfos(state, ownProps.name),
      sortSetting: grantsSortSetting(state),
      dbResponse: databaseDetails(state)[ownProps.name] && databaseDetails(state)[ownProps.name].data,
      grants: grants(state, ownProps.name),
    };
  },
  {
    setUISetting,
    refreshDatabaseDetails,
    refreshTableDetails,
    refreshTableStats,
  },
)(DatabaseSummaryGrants);
