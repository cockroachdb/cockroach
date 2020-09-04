// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, {forwardRef, useCallback, useImperativeHandle, useState} from "react";
import {Modal, Text} from "src/components";
import {Action, Dispatch} from "redux";
import {AdminUIState} from "src/redux/state";
import {connect} from "react-redux";
import {terminateSessionAction} from "src/redux/sessions/sessionsSagas";
import {cockroach} from "src/js/protos";
import ICancelSessionRequest = cockroach.server.serverpb.ICancelSessionRequest;
import {trackTerminateSession} from "src/util/analytics/trackTerminate";

export interface TerminateSessionModalRef {
  showModalFor: (req: ICancelSessionRequest) => void;
}

// tslint:disable-next-line:variable-name
const TerminateSessionModal = (props: TerminateSessionModalProps, ref: React.RefObject<TerminateSessionModalRef>) => {
  const {cancel} = props;
  const [visible, setVisible] = useState(false);
  const [req, setReq] = useState<ICancelSessionRequest>();

  const onOkHandler = useCallback(
    () => {
      cancel(req);
      trackTerminateSession();
      setVisible(false);
    },
    [req],
  );

  const onCancelHandler = useCallback(() => setVisible(false), []);

  useImperativeHandle(ref, () => {
    return {
      showModalFor: (r: ICancelSessionRequest) => {
        setReq(r);
        setVisible(true);
      },
    };
  });

  return (
    <Modal
      visible={visible}
      onOk={onOkHandler}
      onCancel={onCancelHandler}
      okText="Yes"
      cancelText="No"
      title="Terminate the Session?"
    >
      <Text>
        Terminating a session ends the session, terminating its associated connection.
        The client that holds this session will receive a "connection terminated" event.
      </Text>
    </Modal>
  );
};

interface TerminateSessionModalProps {
  cancel: (req: ICancelSessionRequest) => void;
}

const mapDispatchToProps = (dispatch: Dispatch<Action, AdminUIState>): TerminateSessionModalProps => ({
  cancel: (req: ICancelSessionRequest) => {
    dispatch(terminateSessionAction(req));
  },
});

export default connect<null, TerminateSessionModalProps>(
  null,
  mapDispatchToProps,
  null,
  {forwardRef: true},
)(forwardRef(TerminateSessionModal));
