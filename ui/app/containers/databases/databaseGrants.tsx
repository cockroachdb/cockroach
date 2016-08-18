import _ from "lodash";
import * as React from "react";
import { IInjectedProps } from "react-router";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import * as protos from "../../js/protos";
import { databaseNameAttr, tableNameAttr } from "../../util/constants";

import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { refreshDatabaseDetails, refreshTableDetails, generateTableID, CachedDataReducerState } from "../../redux/apiReducers";

import { SortableTable, SortableColumn, SortSetting } from "../../components/sortabletable";

type DatabaseDetailsResponseMessage = cockroach.server.serverpb.DatabaseDetailsResponseMessage;
type TableDetailsResponseMessage = cockroach.server.serverpb.TableDetailsResponseMessage;
type Grant = cockroach.server.serverpb.DatabaseDetailsResponse.Grant | cockroach.server.serverpb.TableDetailsResponse.Grant;

// Constants used to store per-page sort settings in the redux UI store.
const UI_DATABASE_GRANTS_SORT_SETTING_KEY = "databaseDetails/sort_setting/grants";

/******************************
 *      GRANT COLUMN DEFINITION
 */

/**
 * GrantsTableColumn provides an enumeration value for each column in the grants table.
 */
enum GrantsTableColumn {
  User = 1,
  Grants,
}

/**
 * GrantsColumnDescriptor is used to describe metadata about an individual column
 * in the Grants table.
 */
interface GrantsColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: GrantsTableColumn;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (g: Grant) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // Grants. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: Grant) => string;
}

/**
 * grantsColumnDescriptors describes all columns that appear in the grants table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let grantsColumnDescriptors: GrantsColumnDescriptor[] = [
  {
    key: GrantsTableColumn.User,
    title: "User",
    cell: (grants) => grants.user,
    sort: (grants) => grants.user,
  },
  {
    key: GrantsTableColumn.Grants,
    title: "Grants",
    cell: (grants) => grants.privileges.join(", "),
    sort: (grants) => grants.privileges.join(", "),
  },
];

/******************************
 *   DATABASE MAIN COMPONENT
 */

/**
 * DatabaseMainData are the data properties which should be passed to the DatabaseMain
 * container.
 */

interface DatabaseMainData {
  // Current sort setting for the grant table. Incoming rows will already be sorted
  // according to this setting.
  grantsSortSetting: SortSetting;
  // A list of grants, which are possibly sorted according to
  // sortSetting.
  sortedGrants: Grant[];
}

/**
 * DatabaseMainActions are the action dispatchers which should be passed to the
 * DatabaseMain container.
 */
interface DatabaseMainActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting: typeof setUISetting;
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
  refreshTableDetails: typeof refreshTableDetails;
}

/**
 * DatabaseMainProps is the type of the props object that must be passed to
 * DatabaseMain component.
 */
type DatabaseMainProps = DatabaseMainData & DatabaseMainActions & IInjectedProps;

/**
 * DatabaseMain renders the main content of the database details page, which is primarily a
 * data table of all tables and grants.
 */
class DatabaseMain extends React.Component<DatabaseMainProps, {}> {
  /**
   * grantColumns is a selector which computes the input Columns to the grants table,
   * based on the tableColumnDescriptors and the current sorted table data
   */
  grantColumns = createSelector(
    (props: DatabaseMainProps) => props.sortedGrants,
    (grants: Grant[]) => {
      return _.map(grantsColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(grants[index]),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

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
          <SortableTable count={sortedGrants.length}
            sortSetting={grantsSortSetting}
            onChangeSortSetting={(setting) => this.changeGrantSortSetting(setting) }
            columns={this.grantColumns(this.props) }/>
        </div>;
    }
    return <div>No results.</div>;
  }
}

/******************************
 *         SELECTORS
 */

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

// Selectors which sorts statuses according to current sort setting.
let grantsSortFunctionLookup = _(grantsColumnDescriptors).keyBy("key").mapValues<(s: Grant) => any>("sort").value();

// Sorted grants
let sortedGrants = createSelector(
  grants,
  grantsSortSetting,
  (g, sort) => {
    let sortFn = grantsSortFunctionLookup[sort.sortKey];
    if (sort && sortFn) {
      return _.orderBy(g, sortFn, sort.ascending ? "asc" : "desc");
    } else {
      return g;
    }
  });

// Connect the DatabaseMain class with our redux store.
let databaseMainConnected = connect(
  (state: AdminUIState, ownProps: IInjectedProps) => {
    return {
      sortedGrants: sortedGrants(state, ownProps),
      grantsSortSetting: grantsSortSetting(state),
    };
  },
  {
    setUISetting,
    refreshDatabaseDetails,
    refreshTableDetails,
  }
)(DatabaseMain);

export default databaseMainConnected;
