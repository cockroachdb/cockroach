// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Modal, Button } from "antd";
import React, { Fragment } from "react";
import "./styles.styl";
import { ModalProps } from "antd/lib/modal";

interface ICustomModalProps extends ModalProps {
  children?: React.ReactNode;
  trigger?: React.ReactChildren | React.ReactNode;
  triggerStyle?: string;
  triggerTitle?: string;
}

interface ICustomModalState {
  visible: boolean;
}

class CustomModal extends React.Component<
  ICustomModalProps,
  ICustomModalState
> {
  state = { visible: false };

  showModal = () => {
    this.setState({
      visible: true,
    });
  };

  handleOk = () => {
    this.setState({
      visible: false,
    });
  };

  handleCancel = () => {
    this.setState({
      visible: false,
    });
  };

  render() {
    const {
      trigger,
      visible,
      children,
      triggerStyle,
      triggerTitle,
    } = this.props;
    return (
      <Fragment>
        {trigger ? (
          trigger
        ) : (
          <a onClick={this.showModal} className={`${triggerStyle}`}>
            {triggerTitle}
          </a>
        )}
        <Modal
          visible={trigger ? visible : this.state.visible}
          onOk={this.handleOk}
          onCancel={this.handleCancel}
          className="custom--modal"
          maskStyle={{
            background: "rgba(71, 88, 114, 0.73)",
          }}
          footer={
            <Button
              type="link"
              className="custom--modal__close--button"
              onClick={this.handleCancel}
            >
              Done
            </Button>
          }
          {...this.props}
        >
          {children}
        </Modal>
      </Fragment>
    );
  }
}

export default CustomModal;
