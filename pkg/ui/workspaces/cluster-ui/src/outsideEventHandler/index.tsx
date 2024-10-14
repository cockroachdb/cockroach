// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React from "react";

import styles from "./outsideEventHandler.module.scss";

const cx = classNames.bind(styles);

export interface OutsideEventHandlerProps {
  onOutsideClick: () => void;
  children: React.ReactNode;
  mountNodePosition?: "fixed" | "initial";
  ignoreClickOnRefs?: React.RefObject<HTMLDivElement>[];
}

export class OutsideEventHandler extends React.Component<OutsideEventHandlerProps> {
  nodeRef: React.RefObject<HTMLDivElement>;

  constructor(props: OutsideEventHandlerProps) {
    super(props);
    this.nodeRef = React.createRef();
  }

  componentDidMount(): void {
    this.addEventListener();
  }

  componentWillUnmount(): void {
    this.removeEventListener();
  }

  onClick = (event: MouseEvent): void => {
    if (!(event.target instanceof Node)) {
      return;
    }
    const target = event.target;

    const { onOutsideClick, ignoreClickOnRefs = [] } = this.props;
    const isChildEl =
      this.nodeRef.current && this.nodeRef.current.contains(target);

    const isOutsideIgnoredEl = ignoreClickOnRefs.some(outsideIgnoredRef => {
      if (!outsideIgnoredRef || !outsideIgnoredRef.current) {
        return false;
      }
      return outsideIgnoredRef.current.contains(target);
    });

    if (!isChildEl && !isOutsideIgnoredEl) {
      onOutsideClick();
    }
  };

  addEventListener = (): void => {
    document.addEventListener("click", this.onClick);
  };

  removeEventListener = (): void => {
    document.removeEventListener("click", this.onClick);
  };

  render(): React.ReactElement {
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
