import * as React from "react";
import { RouterState, Link } from "react-router";
import { connect } from "react-redux";

import * as protos from "../../js/protos";
import { databaseNameAttr, tableNameAttr } from "../../util/constants";
import { Bytes } from "../../util/format";
import { AdminUIState } from "../../redux/state";
import { LocalSetting } from "../../redux/localsettings";
import { refreshTableDetails, refreshTableStats, generateTableID } from "../../redux/apiReducers";
import { SummaryBar, SummaryHeadlineStat } from "../../components/summaryBar";

import { TableInfo } from "./data";
import { SortSetting } from "../../components/sortabletable";
import { SortedTable } from "../../components/sortedtable";
import * as hljs from "highlight.js";

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
export const GrantsSortedTable = SortedTable as new () => SortedTable<protos.cockroach.server.serverpb.TableDetailsResponse.Grant$Properties>;

const databaseTableGrantsSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "tableDetails/sort_setting/grants", (s) => s.localSettings,
);

/**
 * TableMainData are the data properties which should be passed to the TableMain
 * container.
 */
interface TableMainData {
  tableInfo: TableInfo;
  grantsSortSetting: SortSetting;
}

/**
 * TableMainActions are the action dispatchers which should be passed to the
 * TableMain container.
 */
interface TableMainActions {
  // Refresh the table data
  refreshTableDetails: typeof refreshTableDetails;
  refreshTableStats: typeof refreshTableStats;
  setSort: typeof databaseTableGrantsSortSetting.set;
}

/**
 * TableMainProps is the type of the props object that must be passed to
 * TableMain component.
 */
type TableMainProps = TableMainData & TableMainActions & RouterState;

/**
 * TableMain renders the main content of the databases page, which is primarily a
 * data table of all databases.
 */
class TableMain extends React.Component<TableMainProps, {}> {
  createStmtNode: Node;

  componentWillMount() {
    this.props.refreshTableDetails(new protos.cockroach.server.serverpb.TableDetailsRequest({
      database: this.props.params[databaseNameAttr],
      table: this.props.params[tableNameAttr],
    }));
    this.props.refreshTableStats(new protos.cockroach.server.serverpb.TableStatsRequest({
      database: this.props.params[databaseNameAttr],
      table: this.props.params[tableNameAttr],
    }));
  }

  componentDidMount() {
    hljs.highlightBlock(this.createStmtNode);
  }

  render() {
    let { tableInfo, grantsSortSetting } = this.props;

    if (tableInfo) {
      return <div>
        <section className="section parent-link">
          <Link to="/databases/tables">&lt; Back to Databases</Link>
        </section>
        <section className="section">
          <div className="database-summary-title">
            { this.props.params[tableNameAttr] }
          </div>
          <div className="content l-columns">
            <div className="l-columns__left">
              <pre className="sql" ref={(node) => this.createStmtNode = node}>
                {/* TODO (mrtracy): format and highlight create table statement */}
                {tableInfo.createStatement}
              </pre>
              <div className="sql-table">
                <GrantsSortedTable
                  data={tableInfo.grants}
                  sortSetting={grantsSortSetting}
                  onChangeSortSetting={(setting) => this.props.setSort(setting) }
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
              </div>
            </div>
            <div className="l-columns__right">
              <SummaryBar>
                <SummaryHeadlineStat
                  title="Size"
                  tooltip="Total disk size of this table."
                  value={ tableInfo.size }
                  format={ Bytes }/>
                <SummaryHeadlineStat
                  title="Ranges"
                  tooltip="The total count of ranges in this database"
                  value={ tableInfo.rangeCount }/>
              </SummaryBar>
            </div>
          </div>
        </section>
      </div>;
    }
    return <div>No results.</div>;
  }
}

/******************************
 *         SELECTORS
 */

function tableInfo(state: AdminUIState, props: RouterState): TableInfo {
  let db = props.params[databaseNameAttr];
  let table = props.params[tableNameAttr];
  let details = state.cachedData.tableDetails[generateTableID(db, table)];
  let stats = state.cachedData.tableStats[generateTableID(db, table)];
  return new TableInfo(table, details && details.data, stats && stats.data);
}

// Connect the TableMain class with our redux store.
let tableMainConnected = connect(
  (state: AdminUIState, ownProps: RouterState) => {
    return {
      tableInfo: tableInfo(state, ownProps),
      grantsSortSetting: databaseTableGrantsSortSetting.selector(state),
    };
  },
  {
    setSort: databaseTableGrantsSortSetting.set,
    refreshTableDetails,
    refreshTableStats,
  },
)(TableMain);

export default tableMainConnected;
