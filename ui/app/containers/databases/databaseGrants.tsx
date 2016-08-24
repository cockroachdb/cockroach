import * as React from "react";
import { IInjectedProps } from "react-router";
import { connect } from "react-redux";

import * as protos from "../../js/protos";
import { databaseNameAttr, tableNameAttr } from "../../util/constants";

import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { refreshDatabaseDetails, refreshTableDetails, generateTableID, CachedDataReducerState } from "../../redux/apiReducers";

import { SortSetting } from "../../components/sortabletable";
import { SortedTable } from "../../components/sortedtable";

type DatabaseDetailsResponseMessage = cockroach.server.serverpb.DatabaseDetailsResponseMessage;
type TableDetailsResponseMessage = cockroach.server.serverpb.TableDetailsResponseMessage;
type Grant = cockroach.server.serverpb.DatabaseDetailsResponse.Grant | cockroach.server.serverpb.TableDetailsResponse.Grant;

// Constants used to store per-page sort settings in the redux UI store.
const UI_DATABASE_GRANTS_SORT_SETTING_KEY = "databaseDetails/sort_setting/grants";

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const DatabaseGrantsSortedTable = SortedTable as new () => SortedTable<Grant>;

/**
 * DatabaseGrantsData are the data properties which should be passed to the DatabaseGrants
 * container.
 */
interface DatabaseGrantsData {
  // Current sort setting for the grant table.
  grantsSortSetting: SortSetting;
  // A list of grants for the selected database.
  sortedGrants: Grant[];
}

/**
 * DatabaseGrantsActions are the action dispatchers which should be passed to the
 * DatabaseGrants container.
 */
interface DatabaseGrantsActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting: typeof setUISetting;
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
  refreshTableDetails: typeof refreshTableDetails;
}

/**
 * DatabaseGrantsProps is the type of the props object that must be passed to
 * DatabaseGrants component.
 */
type DatabaseGrantsProps = DatabaseGrantsData & DatabaseGrantsActions & IInjectedProps;

/**
 * DatabaseGrants renders the grants tabof the database details page.
 */
class DatabaseGrants extends React.Component<DatabaseGrantsProps, {}> {
  // Callback when the user elects to change the grant table sort setting.
  changeGrantSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASE_GRANTS_SORT_SETTING_KEY, setting);
  }

  // Refresh when the component is mounted.
  componentWillMount() {
    if (this.props.params[tableNameAttr]) {
      this.props.refreshTableDetails(new protos.cockroach.server.serverpb.TableDetailsRequest({ database: this.props.params[databaseNameAttr], table: this.props.params[tableNameAttr] }));
    } else {
      this.props.refreshDatabaseDetails(new protos.cockroach.server.serverpb.DatabaseDetailsRequest({ database: this.props.params[databaseNameAttr] }));
    }
  }

  render() {
    let { sortedGrants, grantsSortSetting } = this.props;

    if (sortedGrants) {
      return <div className="sql-table">
        <DatabaseGrantsSortedTable
          data={sortedGrants}
          sortSetting={grantsSortSetting}
          onChangeSortSetting={(setting) => this.changeGrantSortSetting(setting) }
          columns={[
            {
              title: "User",
              cell: (grants) => grants.user,
              sort: (grants) => grants.user,
            },
            {
              title: "Grants",
              cell: (grants) => grants.privileges.join(", "),
              sort: (grants) => grants.privileges.join(", "),
            },
          ]}/>
      </div>;
    }
    return <div>No results.</div>;
  }
}

// Base selectors to extract data from redux state.
function grants(state: AdminUIState, props: IInjectedProps): Grant[] {
  let details: CachedDataReducerState<DatabaseDetailsResponseMessage | TableDetailsResponseMessage>;
  if (props.params[tableNameAttr]) {
    details = state.cachedData.tableDetails[generateTableID(props.params[databaseNameAttr], props.params[tableNameAttr])];
  } else {
    details = state.cachedData.databaseDetails[props.params[databaseNameAttr]];
  }
  return details && details.data && details.data.grants || [];
}
let grantsSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASE_GRANTS_SORT_SETTING_KEY] || {};

// Connect the DatabaseGrants class with our redux store.
let databaseGrantsConnected = connect(
  (state: AdminUIState, ownProps: IInjectedProps) => {
    return {
      sortedGrants: grants(state, ownProps),
      grantsSortSetting: grantsSortSetting(state),
    };
  },
  {
    setUISetting,
    refreshDatabaseDetails,
    refreshTableDetails,
  }
)(DatabaseGrants);

export default databaseGrantsConnected;
