// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";

import styles from "./outsideEventHandler.module.scss";

const cx = classNames.bind(styles);

export interface OutsideEventHandlerProps {
  onOutsideClick: () => void;
  children: any;
  mountNodePosition?: "fixed" | "initial";
  ignoreClickOnRefs?: React.RefObject<HTMLDivElement>[];
}

export class OutsideEventHandler extends React.Component<
  OutsideEventHandlerProps
> {
  nodeRef: React.RefObject<HTMLDivElement>;

  constructor(props: any) {
    super(props);
    this.nodeRef = React.createRef();
  }

  componentDidMount() {
    this.addEventListener();
  }

  componentWillUnmount() {
    this.removeEventListener();
  }

  onClick = (event: any) => {
    const { onOutsideClick, ignoreClickOnRefs = [] } = this.props;
    const isChildEl =
      this.nodeRef.current && this.nodeRef.current.contains(event.target);

    const isOutsideIgnoredEl = ignoreClickOnRefs.some(outsideIgnoredRef => {
      if (!outsideIgnoredRef || !outsideIgnoredRef.current) {
        return false;
      }
      return outsideIgnoredRef.current.contains(event.target);
    });

    if (!isChildEl && !isOutsideIgnoredEl) {
      onOutsideClick();
    }
  };

  addEventListener = () => {
    addEventListener("click", this.onClick);
  };

  removeEventListener = () => {
    removeEventListener("click", this.onClick);
  };

  render() {
    const { children, mountNodePosition = "initial" } = this.props;
    const classes = cx(
      "outside-event-handler",
      `outside-event-handler--position-${mountNodePosition}`,
    );

    return (
      <div ref={this.nodeRef} className={classes}>
        {children}
      </div>
    );
  }
}
