import React from "react";
import classNames from "classnames/bind";
import { Modal as AntModal } from "antd";
import { Button } from "../button";
import { Text, TextTypes } from "../text";
import styles from "./modal.module.scss";

export interface ModalProps {
  title?: string;
  onOk?: () => void;
  onCancel?: () => void;
  okText?: string;
  cancelText?: string;
  visible: boolean;
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
}) => {
  return (
    <AntModal
      title={title && <Text textType={TextTypes.Heading3}>{title}</Text>}
      className={cx("crl-modal")}
      visible={visible}
      closeIcon={
        <div className={cx("crl-modal__close-icon")} onClick={onCancel}>
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
