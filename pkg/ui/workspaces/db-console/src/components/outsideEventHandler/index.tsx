// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames";
import React from "react";

import "./outsideEventHandler.styl";

export interface OutsideEventHandlerProps {
  onOutsideClick: () => void;
  children: any;
  mountNodePosition?: "fixed" | "initial";
  ignoreClickOnRefs?: React.RefObject<HTMLDivElement>[];
}

export class OutsideEventHandler extends React.Component<OutsideEventHandlerProps> {
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
    const classes = classNames(
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
