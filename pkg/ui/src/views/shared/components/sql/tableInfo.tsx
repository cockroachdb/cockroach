// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Tooltip } from "antd";
import { AdminUIState } from "oss/src/redux/state";
import { databaseNameAttr, tableNameAttr } from "oss/src/util/constants";
import { selectTableInfo } from "oss/src/views/databases/containers/tableDetails";
import { TableInfo } from "oss/src/views/databases/data/tableInfo";
import React from "react";
import { connect } from "react-redux";
import * as protos from "src/js/protos";
import { refreshTableDetails, refreshTableStats } from "src/redux/apiReducers";
import { Highlight } from "./highlight";
import { Link } from "react-router";

interface TableInfoComponentProps {
  title: any;
  params: {
    database_name: string;
    table_name: string;
  };
}
interface TableInfoProps {
  tableInfo: TableInfo;
  refreshTableDetails: typeof refreshTableDetails;
  refreshTableStats: typeof refreshTableStats;
}

class TableInfoComponent extends React.Component<TableInfoComponentProps & TableInfoProps> {
  componentDidMount() {
    this.loadTable();
  }

  loadTable = () => {
    this.props.refreshTableDetails(new protos.cockroach.server.serverpb.TableDetailsRequest({
      database: this.props.params[databaseNameAttr],
      table: this.props.params[tableNameAttr],
    }));
    this.props.refreshTableStats(new protos.cockroach.server.serverpb.TableStatsRequest({
      database: this.props.params[databaseNameAttr],
      table: this.props.params[tableNameAttr],
    }));
  }

  render() {
    const { title, tableInfo, params } = this.props;
    return (
      <Tooltip overlayClassName="hljs" placement="bottom" title={<pre className="sql-highlight hljs"><Highlight value={tableInfo.createStatement} /></pre>}>
        <Link className="table-target" to={`database/${params[databaseNameAttr]}/table/${params[tableNameAttr]}`}>
          {title}
        </Link>
      </Tooltip>
    );
  }
}

const mapStateToProps = (state: AdminUIState, props: any) => {
  return ({
    tableInfo: selectTableInfo(state, props),
  });
};

const mapDispatchToProps = {
  refreshTableDetails,
  refreshTableStats,
};

// tslint:disable-next-line:variable-name
const TableInfoConnected = connect(
  mapStateToProps,
  mapDispatchToProps,
)(TableInfoComponent);

export default TableInfoConnected;
