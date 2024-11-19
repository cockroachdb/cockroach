// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Modal as AntModal } from "antd";
import React from "react";

import "antd/lib/modal/style";
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

export const Modal: React.FC<ModalProps> = props => {
  const { children, onOk, onCancel, okText, cancelText, visible, title } =
    props;
  return (
    <AntModal
      title={title && <Text textType={TextTypes.Heading3}>{title}</Text>}
      className="crl-modal"
      open={visible}
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
