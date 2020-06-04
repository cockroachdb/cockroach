// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { expect } from "chai";
import { shallow } from "enzyme";
import { ArrowDirection } from "oss/src/views/shared/components/dropdown";
import React from "react";
import sinon from "sinon";
import "src/enzymeInit";
import TimeFrameControls from "../../components/controls";
import { RangeSelectProps } from "./index";

describe("<TimeFrameControls>", function() {
  let spy: sinon.SinonSpy;

  const makeTimeScaleDropdown = (props: RangeSelectProps) => shallow((<TimeFrameControls {...props} />));

  beforeEach(function() {
    spy = sinon.spy();
  });

  it("must return 2 disabled button.", () => {
    const wrapper = makeTimeScaleDropdown({ disabledArrows: [ArrowDirection.CENTER, ArrowDirection.RIGHT], onArrowClick: spy });
    const button = wrapper.find("._action.disabled");
    expect(button.length).to.equal(2);
  });

  it("must return 0 disabled button.", () => {
    const wrapper = makeTimeScaleDropdown({ disabledArrows: [], onArrowClick: spy });
    const button = wrapper.find("._action.disabled");
    expect(button.length).to.equal(0);
  });

});