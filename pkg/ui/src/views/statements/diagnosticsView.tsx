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
import { connect } from "react-redux";

import { Button, Text, TextTypes, Anchor } from "src/components";
import { AdminUIState } from "src/redux/state";
import { SummaryCard } from "src/views/shared/components/summaryCard";

import "./diagnosticsView.styl";
import { requestStatementDiagnostics } from "oss/src/redux/statements";
import { selectDiagnosticRequestsCountByStatementFingerprint } from "oss/src/redux/statements/statementsSelectors";

interface DiagnosticsViewOwnProps {
  statementFingerprint?: string;
}

type DiagnosticsViewProps = DiagnosticsViewOwnProps & MapStateToProps & MapDispatchToProps;

class DiagnosticsView extends React.Component<DiagnosticsViewProps> {

  render() {
    const { hasData } = this.props;

    if (!hasData) {
      return (
        <SummaryCard className="summary--card__empty-sate">
          <EmptyDiagnosticsView {...this.props} />
        </SummaryCard>
      );
    }
    return (
      <SummaryCard className="">
        <h2 className="base-heading summary--card__title">
          Execution Latency By Phase
        </h2>
      </SummaryCard>
    );
  }
}

export class EmptyDiagnosticsView extends React.Component<DiagnosticsViewProps> {

  onActivateButtonClick = () => {
    const { activate, statementFingerprint } = this.props;
    activate(statementFingerprint);
  }

  render() {
    return (
      <div className="crl-statements-diagnostics-view">
        <Text
          className="crl-statements-diagnostics-view__title"
          textType={TextTypes.Heading3}
        >
          Activate statement diagnostics
        </Text>
        <div className="crl-statements-diagnostics-view__content">
          <main className="crl-statements-diagnostics-view__main">
            <Text
              textType={TextTypes.Body}
            >
              When you activate statement diagnostics, CockroachDB will wait for the next query that matches
              this statement fingerprint. A download button will appear on the statement list and detail pages
              when the query is ready. The statement diagnostic will include EXPLAIN plans,
              table statistics, and traces. <Anchor href="https://www.cockroachlabs.com/docs/stable">Learn more</Anchor>
            </Text>
          {/*  TODO (koorosh): change Learn more link to something meaningful ^^^. */}
          </main>
          <footer className="crl-statements-diagnostics-view__footer">
            <Button
              type="primary"
              onClick={this.onActivateButtonClick}
            >
              Activate
            </Button>
          </footer>
        </div>
      </div>
    );
  }
}

interface MapStateToProps {
  hasData: boolean;
}

interface MapDispatchToProps {
  activate: (statementFingerprint: string) => void;
}

const mapStateToProps = (state: AdminUIState, props: DiagnosticsViewProps): MapStateToProps => ({
  hasData: selectDiagnosticRequestsCountByStatementFingerprint(state, props.statementFingerprint) > 0,
});

const mapDispatchToProps: MapDispatchToProps = {
  activate: requestStatementDiagnostics,
};

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  DiagnosticsViewOwnProps
  >(mapStateToProps, mapDispatchToProps)(DiagnosticsView);
