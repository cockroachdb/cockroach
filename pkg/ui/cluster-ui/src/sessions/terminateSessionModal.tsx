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
  useCallback,
  useImperativeHandle,
  useState,
} from "react";
import { Modal } from "../modal";
import { Text } from "../text";

//import {cockroach} from "src/js/protos";
//import ICancelSessionRequest = cockroach.server.serverpb.ICancelSessionRequest;
type ICancelSessionRequest = any;
//import {trackTerminateSession} from "src/util/analytics/trackTerminate";

export interface TerminateSessionModalRef {
  showModalFor: (req: ICancelSessionRequest) => void;
}

interface TerminateSessionModalProps {
  //cancel: (req: ICancelSessionRequest) => void;
  cancel?: (req: ICancelSessionRequest) => void;
}

// tslint:disable-next-line:variable-name
const TerminateSessionModal = (
  props: TerminateSessionModalProps,
  ref: React.RefObject<TerminateSessionModalRef>,
) => {
  const { cancel } = props;
  const [visible, setVisible] = useState(false);
  const [req, setReq] = useState<ICancelSessionRequest>();

  const onOkHandler = useCallback(() => {
    cancel(req);
    //trackTerminateSession();
    setVisible(false);
  }, [req, cancel]);

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
        Terminating a session ends the session, terminating its associated
        connection. The client that holds this session will receive a
        &quot;connection terminated&quot; event.
      </Text>
    </Modal>
  );
};

export default forwardRef(TerminateSessionModal);
