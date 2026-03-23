// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames";
import React, { useRef } from "react";

import { OutsideEventHandler } from "src/components/outsideEventHandler";

import "./popover.scss";

export interface PopoverProps {
  content: JSX.Element;
  visible: boolean;
  onVisibleChange: (nextState: boolean) => void;
  children: any;
}

function Popover({
  content,
  children,
  visible,
  onVisibleChange,
}: PopoverProps): React.ReactElement {
  // contentRef is used to pass as element to avoid handling outside event handler
  // on its instance.
  const contentRef = useRef<HTMLDivElement>(null);

  const popoverClasses = classNames("popover", {
    "popover--visible": visible,
  });

  return (
    <>
      <div
        ref={contentRef}
        className="popover__content"
        onClick={() => onVisibleChange(!visible)}
      >
        {content}
      </div>
      <OutsideEventHandler
        onOutsideClick={() => onVisibleChange(false)}
        mountNodePosition={"fixed"}
        ignoreClickOnRefs={[contentRef]}
      >
        <div className={popoverClasses}>{children}</div>
      </OutsideEventHandler>
    </>
  );
}

export default Popover;
