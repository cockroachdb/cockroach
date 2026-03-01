// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames";
import React, { useEffect, useRef } from "react";

import "./outsideEventHandler.scss";

export interface OutsideEventHandlerProps {
  onOutsideClick: () => void;
  children: any;
  mountNodePosition?: "fixed" | "initial";
  ignoreClickOnRefs?: React.RefObject<HTMLDivElement>[];
}

export function OutsideEventHandler({
  onOutsideClick,
  children,
  mountNodePosition = "initial",
  ignoreClickOnRefs = [],
}: OutsideEventHandlerProps) {
  const nodeRef = useRef<HTMLDivElement>(null);

  // Use a ref to always have access to the latest callback and ignored refs
  // without re-registering the listener on every render.
  const onOutsideClickRef = useRef(onOutsideClick);
  onOutsideClickRef.current = onOutsideClick;
  const ignoreClickOnRefsRef = useRef(ignoreClickOnRefs);
  ignoreClickOnRefsRef.current = ignoreClickOnRefs;

  useEffect(() => {
    const handleClick = (event: MouseEvent) => {
      const isChildEl =
        nodeRef.current && nodeRef.current.contains(event.target as Node);

      const isOutsideIgnoredEl = ignoreClickOnRefsRef.current.some(
        outsideIgnoredRef => {
          if (!outsideIgnoredRef || !outsideIgnoredRef.current) {
            return false;
          }
          return outsideIgnoredRef.current.contains(event.target as Node);
        },
      );

      if (!isChildEl && !isOutsideIgnoredEl) {
        onOutsideClickRef.current();
      }
    };

    addEventListener("click", handleClick);
    return () => removeEventListener("click", handleClick);
  }, []);

  const classes = classNames(
    "outside-event-handler",
    `outside-event-handler--position-${mountNodePosition}`,
  );

  return (
    <div ref={nodeRef} className={classes}>
      {children}
    </div>
  );
}
