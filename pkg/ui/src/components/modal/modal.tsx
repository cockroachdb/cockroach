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
import { Modal as AntModal } from "antd";
import { Button, Text, TextTypes } from "src/components";
import "./modal.styl";

export interface ModalProps {
  title?: string;
  onOk?: () => void;
  onCancel?: () => void;
  okText?: string;
  cancelText?: string;
  visible: boolean;
}

export const Modal: React.FC<ModalProps> = (props) => {
  const {
    children,
    onOk,
    onCancel,
    okText,
    cancelText,
    visible,
    title,
  } = props;
  return (
    <AntModal
      title={title && <Text textType={TextTypes.Heading3}>{title}</Text>}
      className="crl-modal"
      visible={visible}
      closeIcon={
        <div className="crl-modal__close-icon" onClick={onCancel}>
          &times;
        </div>
      }
      footer={[
        <Button onClick={onCancel} type="secondary" key="cancelButton">
          {cancelText}
        </Button>,
        <Button onClick={onOk} type="primary" key="okButton">
          {okText}
        </Button>,
      ]}
    >
      {children}
    </AntModal>
  );
};
