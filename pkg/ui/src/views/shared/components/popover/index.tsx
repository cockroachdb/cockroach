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

import { OutsideEventHandler } from "src/views/shared/components/outsideEventHandler";

import "./popover.styl";

export interface PopoverProps {
  content: JSX.Element;
  visible: boolean;
  onVisibleChange: (nextState: boolean) => void;
  children: any;
}

export default class Popover extends React.Component<PopoverProps> {
  // contentRef is used to pass as element to avoid handling outside event handler
  // on its instance.
  contentRef: React.RefObject<HTMLDivElement> = React.createRef();

  render() {
    const { content, children, visible, onVisibleChange } = this.props;

    const popoverClasses = classNames("popover", {
      "popover--visible": visible,
    });

    return (
      <React.Fragment>
        <div
          ref={this.contentRef}
          className="popover__content"
          onClick={() => onVisibleChange(!visible)}>
          { content }
        </div>
        <OutsideEventHandler
          onOutsideClick={() => onVisibleChange(false)}
          mountNodePosition={"fixed"}
          ignoreClickOnRefs={[this.contentRef]}>
          <div className={popoverClasses}>
            { children }
          </div>
        </OutsideEventHandler>
      </React.Fragment>
    );
  }
}
