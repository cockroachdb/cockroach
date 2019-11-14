// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import React from "react";
import classNames from "classnames";

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
    const isChildEl = this.nodeRef.current && this.nodeRef.current.contains(event.target);

    const isOutsideIgnoredEl = ignoreClickOnRefs.some(outsideIgnoredRef => {
      if (!outsideIgnoredRef || !outsideIgnoredRef.current) {
        return false;
      }
      return outsideIgnoredRef.current.contains(event.target);
    });

    if (!isChildEl && !isOutsideIgnoredEl) {
      onOutsideClick();
    }
  }

  addEventListener = () => {
    addEventListener("click", this.onClick);
  }

  removeEventListener = () => {
    removeEventListener("click", this.onClick);
  }

  render() {
    const { children, mountNodePosition = "initial" } = this.props;
    const classes = classNames("outside-event-handler", `outside-event-handler--position-${mountNodePosition}`);

    return (
      <div ref={ this.nodeRef } className={classes}>
        { children }
      </div>
    );
  }
}
