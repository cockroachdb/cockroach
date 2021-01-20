// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Col, Row, Tabs } from "antd";
import { SummaryCard } from "src/views/shared/components/summaryCard";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import * as protos from "src/js/protos";
import {
  generateTableID,
  refreshTableDetails,
  refreshTableStats,
  refreshDatabaseDetails,
} from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { databaseNameAttr, tableNameAttr } from "src/util/constants";
import { Bytes } from "src/util/format";
import { TableInfo } from "src/views/databases/data/tableInfo";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
const { TabPane } = Tabs;
import { getMatchParamByName } from "src/util/query";
import { Button } from "@cockroachlabs/cluster-ui";
import { ArrowLeft } from "@cockroachlabs/icons";
import SqlBox from "src/views/shared/components/sql/box";

class GrantsSortedTable extends SortedTable<protos.cockroach.server.serverpb.TableDetailsResponse.IGrant> {}

const databaseTableGrantsSortSetting = new LocalSetting<
  AdminUIState,
  SortSetting
>("tableDetails/sort_setting/grants", (s) => s.localSettings);

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
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
  setSort: typeof databaseTableGrantsSortSetting.set;
}

/**
 * TableMainProps is the type of the props object that must be passed to
 * TableMain component.
 */
type TableMainProps = TableMainData & TableMainActions & RouteComponentProps;

/**
 * TableMain renders the main content of the databases page, which is primarily a
 * data table of all databases.
 */
export class TableMain extends React.Component<TableMainProps, {}> {
  componentDidMount() {
    const database = getMatchParamByName(this.props.match, databaseNameAttr);
    const table = getMatchParamByName(this.props.match, tableNameAttr);
    this.props.refreshDatabaseDetails(
      new protos.cockroach.server.serverpb.DatabaseDetailsRequest({
        database: getMatchParamByName(this.props.match, databaseNameAttr),
      }),
    );
    this.props.refreshTableDetails(
      new protos.cockroach.server.serverpb.TableDetailsRequest({
        database,
        table,
      }),
    );
    this.props.refreshTableStats(
      new protos.cockroach.server.serverpb.TableStatsRequest({
        database,
        table,
      }),
    );
  }

  prevPage = () => this.props.history.goBack();

  render() {
    const { tableInfo, grantsSortSetting, match } = this.props;
    const database = getMatchParamByName(match, databaseNameAttr);
    const table = getMatchParamByName(match, tableNameAttr);

    const title = `${database}.${table}`;
    if (tableInfo) {
      return (
        <div>
          <Helmet title={`${title} Table | Databases`} />
          <div className="page--header">
            <Button
              onClick={this.prevPage}
              type="unstyled-link"
              size="small"
              icon={<ArrowLeft fontSize={"10px"} />}
              iconPosition="left"
            >
              Databases
            </Button>
            <div className="database-summary-title">
              <h2 className="base-heading">{title}</h2>
            </div>
          </div>
          <section className="section section--container table-details">
            <Tabs defaultActiveKey="1" className="cockroach--tabs">
              <TabPane tab="Overview" key="1">
                <Row gutter={16}>
                  <Col className="gutter-row" span={16}>
                    <SqlBox
                      value={tableInfo.createStatement || ""}
                      secondaryValue={tableInfo.configureZoneStatement || ""}
                    />
                  </Col>
                  <Col className="gutter-row" span={8}>
                    <SummaryCard>
                      <Row>
                        <Col span={12}>
                          <div className="summary--card__counting">
                            <h3 className="summary--card__counting--value">
                              {Bytes(tableInfo.physicalSize)}
                            </h3>
                            <p className="summary--card__counting--label">
                              Size
                            </p>
                          </div>
                        </Col>
                        <Col span={12}>
                          <div className="summary--card__counting">
                            <h3 className="summary--card__counting--value">
                              {tableInfo.numReplicas}
                            </h3>
                            <p className="summary--card__counting--label">
                              Replicas
                            </p>
                          </div>
                        </Col>
                        <Col span={24}>
                          <div className="summary--card__counting">
                            <h3 className="summary--card__counting--value">
                              {tableInfo.rangeCount}
                            </h3>
                            <p className="summary--card__counting--label">
                              Ranges
                            </p>
                          </div>
                        </Col>
                      </Row>
                    </SummaryCard>
                  </Col>
                </Row>
              </TabPane>
              <TabPane tab="Grants" key="2">
                <SummaryCard>
                  <GrantsSortedTable
                    data={tableInfo.grants}
                    sortSetting={grantsSortSetting}
                    onChangeSortSetting={(setting) =>
                      this.props.setSort(setting)
                    }
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
                    ]}
                  />
                </SummaryCard>
              </TabPane>
            </Tabs>
          </section>
        </div>
      );
    }
    return <div>No results.</div>;
  }
}

/******************************
 *         SELECTORS
 */

export function selectTableInfo(
  state: AdminUIState,
  props: RouteComponentProps,
): TableInfo {
  const db = getMatchParamByName(props.match, databaseNameAttr);
  const table = getMatchParamByName(props.match, tableNameAttr);
  const details = state.cachedData.tableDetails[generateTableID(db, table)];
  const stats = state.cachedData.tableStats[generateTableID(db, table)];
  return new TableInfo(table, details && details.data, stats && stats.data);
}

const mapStateToProps = (
  state: AdminUIState,
  ownProps: RouteComponentProps,
) => {
  return {
    tableInfo: selectTableInfo(state, ownProps),
    grantsSortSetting: databaseTableGrantsSortSetting.selector(state),
  };
};

const mapDispatchToProps = {
  setSort: databaseTableGrantsSortSetting.set,
  refreshTableDetails,
  refreshTableStats,
  refreshDatabaseDetails,
};

// Connect the TableMain class with our redux store.
const tableMainConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(TableMain),
);

export default tableMainConnected;
