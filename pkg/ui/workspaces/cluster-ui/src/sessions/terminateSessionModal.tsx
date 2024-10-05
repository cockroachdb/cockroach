// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, {
  forwardRef,
  useCallback,
  useImperativeHandle,
  useState,
} from "react";

import { ICancelSessionRequest } from "src/store/terminateQuery";

import { Modal } from "../modal";
import { Text } from "../text";

export interface TerminateSessionModalRef {
  showModalFor: (req: ICancelSessionRequest) => void;
}

interface TerminateSessionModalProps {
  cancel: (payload: ICancelSessionRequest) => void;
}

const TerminateSessionModal: React.ForwardRefRenderFunction<
  TerminateSessionModalRef,
  TerminateSessionModalProps
> = (props, ref) => {
  const { cancel } = props;
  const [visible, setVisible] = useState(false);
  const [req, setReq] = useState<ICancelSessionRequest>();

  const onOkHandler = useCallback(() => {
    cancel(req);
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
      title="Cancel the Session"
    >
      <Text>
        Cancelling a session ends the session, cancelling its associated
        connection. The client that holds this session will receive a
        &quot;connection terminated&quot; event.
      </Text>
    </Modal>
  );
};

export default forwardRef(TerminateSessionModal);
