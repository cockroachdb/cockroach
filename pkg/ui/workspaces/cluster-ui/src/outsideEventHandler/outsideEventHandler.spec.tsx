// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { createRef } from "react";
import { render, waitFor } from "@testing-library/react";

import { OutsideEventHandler } from "./index";

describe("OutsideEventHandler", () => {
  async function renderComponent() {
    const onOutsideClick = jest.fn();
    const onInsideClick = jest.fn();
    const outside = createRef<HTMLDivElement>();
    const ignored = createRef<HTMLDivElement>();
    const inside = createRef<HTMLButtonElement>();
    render(
      <div>
        <div ref={outside}>outside</div>
        <OutsideEventHandler
          onOutsideClick={onOutsideClick}
          ignoreClickOnRefs={[ignored]}
        >
          <button ref={inside} onClick={onInsideClick}>
            inside
          </button>
        </OutsideEventHandler>
        <div ref={ignored}>ignored</div>
      </div>,
    );

    await waitFor(() => {
      expect(inside.current).toBeTruthy();
      expect(outside.current).toBeTruthy();
      expect(ignored.current).toBeTruthy();
    });

    return {
      onOutsideClick,
      onInsideClick,
      outside,
      ignored,
      inside,
    };
  }

  it("should call onOutsideClick when clicking outside the component", async () => {
    const { onOutsideClick, onInsideClick, outside } = await renderComponent();
    // Click outside the component
    outside.current.click();

    await waitFor(() => {
      expect(onOutsideClick).toHaveBeenCalled();
      expect(onInsideClick).not.toHaveBeenCalled();
    });
  });

  it("should not call onOutsideClick when clicking inside the component", async () => {
    const { onOutsideClick, onInsideClick, inside } = await renderComponent();
    // Click inside the component.
    inside.current.click();

    await waitFor(() => {
      expect(onInsideClick).toHaveBeenCalled();
      expect(onOutsideClick).not.toHaveBeenCalled();
    });
  });

  it("should not call onOutsideClick when clicking on the ignored ref", async () => {
    const { onOutsideClick, ignored } = await renderComponent();
    // Click on the ignored ref.
    ignored.current.click();

    await waitFor(() => {
      expect(onOutsideClick).not.toHaveBeenCalled();
    });
  });
});
