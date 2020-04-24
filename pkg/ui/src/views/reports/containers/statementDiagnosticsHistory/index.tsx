// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import moment from "moment";
import { Action, Dispatch } from "redux";
import Long from "long";

import { Button, DownloadFile, DownloadFileRef, Text, TextTypes } from "src/components";
import HeaderSection from "src/views/shared/components/headerSection";
import { AdminUIState } from "src/redux/state";
import { getStatementDiagnostics } from "src/util/api";
import { trustIcon } from "src/util/trust";
import DownloadIcon from "!!raw-loader!assets/download.svg";
import {
  selectStatementDiagnosticsReports,
} from "src/redux/statements/statementsSelectors";
import {
  invalidateStatementDiagnosticsRequests,
  refreshStatementDiagnosticsRequests,
} from "src/redux/apiReducers";
import { DiagnosticStatusBadge } from "src/views/statements/diagnostics/diagnosticStatusBadge";
import "./statementDiagnosticsHistoryView.styl";
import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
import StatementDiagnosticsRequest = cockroach.server.serverpb.StatementDiagnosticsRequest;
import {
  getDiagnosticsStatus,
} from "src/views/statements/diagnostics";
import { SortedTable, ColumnDescriptor } from "src/views/shared/components/sortedtable";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { statementsTable } from "src/util/docs";

type StatementDiagnosticsHistoryViewProps = MapStateToProps & MapDispatchToProps;

interface StatementDiagnosticsHistoryViewState {
  sortSetting: {
    sortKey: number;
    ascending: boolean;
  };
}

class StatementDiagnosticsHistoryTable extends SortedTable<{}> {}

class StatementDiagnosticsHistoryView extends React.Component<StatementDiagnosticsHistoryViewProps, StatementDiagnosticsHistoryViewState> {
  columns: ColumnDescriptor<IStatementDiagnosticsReport>[] = [
    {
      title: "Activated on",
      cell: record => moment(record.requested_at.seconds.toNumber() * 1000).format("LL[ at ]h:mm a"),
      sort: record => moment(record.requested_at.seconds.toNumber() * 1000),
    },
    {
      title: "Statement",
      cell: record => <Text textType={TextTypes.Code}>{record.statement_fingerprint}</Text>,
      sort: record => record.statement_fingerprint,
    },
    {
      title: "Status",
      sort: record => record.completed.toString(),
      cell: record => (
        <Text>
          <DiagnosticStatusBadge
            status={getDiagnosticsStatus(record)}
          />
        </Text>
      ),
    },
    {
      title: "",
      cell: (record) => {
        if (record.completed) {
          return (
            <div className="crl-statements-diagnostics-view__actions-column cell--show-on-hover nodes-table__link">
              <Button
                onClick={() => this.getStatementDiagnostics(record.statement_diagnostics_id)}
                size="small"
                type="flat"
                iconPosition="left"
                icon={() => (
                  <span
                    className="crl-statements-diagnostics-view__icon"
                    dangerouslySetInnerHTML={ trustIcon(DownloadIcon) }
                  />
                )}
              >
                Download
              </Button>
            </div>
          );
        }
        return null;
      },
    },
  ];

  tablePageSize = 16;

  downloadRef = React.createRef<DownloadFileRef>();

  constructor(props: StatementDiagnosticsHistoryViewProps) {
    super(props);
    this.state = {
      sortSetting: {
        sortKey: 0,
        ascending: true,
      },
    },
    props.refresh();
  }

  renderTableTitle = () => {
    const { diagnosticsReports } = this.props;
    const totalCount = diagnosticsReports.length;

    if (totalCount <= this.tablePageSize) {
      return (
        <div className="diagnostics-history-view__table-header">
          <Text>{`${totalCount} traces`}</Text>
        </div>
      );
    }

    return (
      <div className="diagnostics-history-view__table-header">
        <Text>{`${this.tablePageSize} of ${totalCount} traces`}</Text>
      </div>
    );
  }

  getStatementDiagnostics = async (diagnosticsId: Long) => {
    const request = new StatementDiagnosticsRequest({ statement_diagnostics_id: diagnosticsId });
    const response = await getStatementDiagnostics(request);
    const trace = response.diagnostics?.trace;
    this.downloadRef.current?.download("statement-diagnostics.json", "application/json", trace);
  }

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  }

  render() {
    const { diagnosticsReports } = this.props;
    const dataSource = diagnosticsReports.map((diagnosticsReport, idx) => ({
      ...diagnosticsReport,
      key: idx,
    }));

    return (
      <section className="section">
        <Helmet title="Statement diagnostics history | Debug" />
        <HeaderSection
          title="Statement diagnostics history"
          navigationBackConfig={{
            text: "Advanced Debug",
            path: "/debug",
          }}
        />
        { this.renderTableTitle() }
        <StatementDiagnosticsHistoryTable
          className="statements-table"
          data={dataSource}
          columns={this.columns}
          empty={dataSource.length === 0}
          emptyProps={{
            title: "There are no statement diagnostics to display.",
            description: "Statement diagnostics can help when troubleshooting issues with specific queries. The diagnostic bundle can be activated from individual statement pages and will include EXPLAIN plans, table statistics, and traces.",
            label: "Learn more",
            onClick: () => window.open(statementsTable),
          }}
          sortSetting={this.state.sortSetting}
          onChangeSortSetting={this.changeSortSetting}
        />
        <DownloadFile ref={this.downloadRef}/>
      </section>
    );
  }
}

interface MapStateToProps {
  diagnosticsReports: IStatementDiagnosticsReport[];
}

interface MapDispatchToProps {
  refresh: () => void;
}

const mapStateToProps = (state: AdminUIState): MapStateToProps => ({
  diagnosticsReports: selectStatementDiagnosticsReports(state) || [],
});

const mapDispatchToProps = (dispatch: Dispatch<Action, AdminUIState>): MapDispatchToProps => ({
  refresh: () => {
    dispatch(invalidateStatementDiagnosticsRequests());
    dispatch(refreshStatementDiagnosticsRequests());
  },
});

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  StatementDiagnosticsHistoryViewProps
  >(mapStateToProps, mapDispatchToProps)(StatementDiagnosticsHistoryView);
