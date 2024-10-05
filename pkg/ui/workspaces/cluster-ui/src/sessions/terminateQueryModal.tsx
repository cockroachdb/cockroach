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

import { ICancelQueryRequest } from "src/store/terminateQuery";

import { Modal } from "../modal";
import { Text } from "../text";
export interface TerminateQueryModalRef {
  showModalFor: (req: ICancelQueryRequest) => void;
}

interface TerminateQueryModalProps {
  cancel: (payload: ICancelQueryRequest) => void;
}

const TerminateQueryModal: React.ForwardRefRenderFunction<
  TerminateQueryModalRef,
  TerminateQueryModalProps
> = (props, ref) => {
  const { cancel } = props;
  const [visible, setVisible] = useState(false);
  const [req, setReq] = useState<ICancelQueryRequest>();

  const onOkHandler = useCallback(() => {
    cancel(req);
    setVisible(false);
  }, [req, cancel]);

  const onCancelHandler = useCallback(() => setVisible(false), []);

  useImperativeHandle(ref, () => {
    return {
      showModalFor: (r: ICancelQueryRequest) => {
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
      title="Cancel the Statement"
    >
      <Text>
        Cancelling a statement ends the statement, returning an error to the
        session.
      </Text>
    </Modal>
  );
};

export default forwardRef(TerminateQueryModal);
