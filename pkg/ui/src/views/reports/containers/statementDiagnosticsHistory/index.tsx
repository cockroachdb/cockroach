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

import { Button, ColumnsConfig, DownloadFile, DownloadFileRef, Table, Text, TextTypes } from "src/components";
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
  sortByCompletedField,
  sortByRequestedAtField,
  sortByStatementFingerprintField,
} from "src/views/statements/diagnostics";
import { trackDownloadDiagnosticsBundle } from "src/util/analytics";

type StatementDiagnosticsHistoryViewProps = MapStateToProps & MapDispatchToProps;

class StatementDiagnosticsHistoryView extends React.Component<StatementDiagnosticsHistoryViewProps> {
  columns: ColumnsConfig<IStatementDiagnosticsReport> = [
    {
      key: "activatedOn",
      title: "Activated on",
      sorter: sortByRequestedAtField,
      defaultSortOrder: "descend",
      width: "240px",
      render: (_text, record) => {
        const timestamp = record.requested_at.seconds.toNumber() * 1000;
        return moment(timestamp).format("LL[ at ]h:mm a");
      },
    },
    {
      key: "statement",
      title: "statement",
      sorter: sortByStatementFingerprintField,
      render: (_text, record) => (
        <Text textType={TextTypes.Code}>{record.statement_fingerprint}</Text>
      ),
    },
    {
      key: "status",
      title: "status",
      sorter: sortByCompletedField,
      width: "160px",
      render: (_text, record) => (
        <Text>
          <DiagnosticStatusBadge
            status={getDiagnosticsStatus(record)}
          />
        </Text>
      ),
    },
    {
      key: "actions",
      title: "",
      sorter: false,
      width: "160px",
      render: (_text, record) => {
        if (record.completed) {
          return (
            <div className="crl-statements-diagnostics-view__actions-column cell--show-on-hover nodes-table__link">
              <a href={`_admin/v1/stmtbundle/${record.statement_diagnostics_id}`}
                 onClick={() => trackDownloadDiagnosticsBundle(record.statement_fingerprint)}>
                <Button
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
                  Bundle (.zip)
                </Button>
              </a>
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
    props.refresh();
  }

  renderTableTitle = () => {
    const { diagnosticsReports } = this.props;
    const totalCount = diagnosticsReports.length;

    if (totalCount === 0) {
      return null;
    }

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
        <div className="diagnostics-history-view__table-container">
          <Table
            pageSize={this.tablePageSize}
            dataSource={dataSource}
            columns={this.columns}
          />
        </div>
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
