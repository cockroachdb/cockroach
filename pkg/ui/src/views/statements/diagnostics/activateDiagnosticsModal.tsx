// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, {
  forwardRef,
  useState,
  useCallback,
  useImperativeHandle,
} from "react";
import { connect } from "react-redux";
import { Action, Dispatch } from "redux";

import { Anchor, Modal, Text } from "src/components";
import { createStatementDiagnosticsReportAction } from "src/redux/statements";
import { AdminUIState } from "src/redux/state";
import {
  invalidateStatementDiagnosticsRequests,
  refreshStatementDiagnosticsRequests,
} from "src/redux/apiReducers";
import { statementDiagnostics } from "src/util/docs";
import {
  trackActivateDiagnostics,
  trackDiagnosticsModalOpen,
} from "src/util/analytics";
export type ActivateDiagnosticsModalProps = MapDispatchToProps;

export interface ActivateDiagnosticsModalRef {
  showModalFor: (statement: string) => void;
}

const ActivateDiagnosticsModal = (
  props: ActivateDiagnosticsModalProps,
  ref: React.RefObject<ActivateDiagnosticsModalRef>,
) => {
  const { activate } = props;
  const [visible, setVisible] = useState(false);
  const [statement, setStatement] = useState<string>();

  const onOkHandler = useCallback(() => {
    activate(statement);
    trackActivateDiagnostics(statement);
    setVisible(false);
  }, [activate, statement]);

  const onCancelHandler = useCallback(() => setVisible(false), []);

  useImperativeHandle(ref, () => {
    return {
      showModalFor: (forwardStatement: string) => {
        setStatement(forwardStatement);
        trackDiagnosticsModalOpen(forwardStatement);
        setVisible(true);
      },
    };
  });

  return (
    <Modal
      visible={visible}
      onOk={onOkHandler}
      onCancel={onCancelHandler}
      okText="Activate"
      cancelText="Cancel"
      title="Activate statement diagnostics"
    >
      <Text>
        When you activate statement diagnostics, CockroachDB will wait for the
        next query that matches this statement fingerprint.
      </Text>
      <p />
      <Text>
        A download button will appear on the statement list and detail pages
        when the query is ready. The download will include EXPLAIN plans, table
        statistics, and traces.{" "}
        <Anchor href={statementDiagnostics}>Learn more</Anchor>
      </Text>
    </Modal>
  );
};

interface MapDispatchToProps {
  activate: (statement: string) => void;
  refreshDiagnosticsReports: () => void;
}

const mapDispatchToProps = (
  dispatch: Dispatch<Action, AdminUIState>,
): MapDispatchToProps => ({
  activate: (statement: string) =>
    dispatch(createStatementDiagnosticsReportAction(statement)),
  refreshDiagnosticsReports: () => {
    dispatch(invalidateStatementDiagnosticsRequests());
    dispatch(refreshStatementDiagnosticsRequests());
  },
});

export default connect<null, MapDispatchToProps>(
  null,
  mapDispatchToProps,
  null,
  { forwardRef: true },
)(forwardRef(ActivateDiagnosticsModal));
