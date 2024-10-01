// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Modal as AntModal, Space } from "antd";
import classNames from "classnames/bind";
import React from "react";

import { Button } from "../button";
import SpinIcon from "../icon/spin";
import { Text, TextTypes } from "../text";

import styles from "./modal.module.scss";

export interface ModalProps {
  title?: string;
  onOk?: () => void;
  onCancel?: () => void;
  okText?: string;
  cancelText?: string;
  visible: boolean;
  className?: string;
  okLoading?: boolean;
}

const cx = classNames.bind(styles);

export const Modal: React.FC<ModalProps> = ({
  children,
  onOk,
  onCancel,
  okText,
  cancelText,
  visible,
  title,
  className,
  okLoading,
}) => {
  return (
    <AntModal
      title={title && <Text textType={TextTypes.Heading3}>{title}</Text>}
      className={cx("crl-modal", className)}
      open={visible}
      closable
      onCancel={onCancel}
      footer={
        <Space>
          <Button onClick={onCancel} type="secondary" key="cancelButton">
            {cancelText}
          </Button>
          <Button
            onClick={onOk}
            type="primary"
            key="okButton"
            icon={okLoading ? <SpinIcon width={15} height={15} /> : undefined}
            disabled={okLoading}
          >
            {okText}
          </Button>
        </Space>
      }
    >
      {children}
    </AntModal>
  );
};
