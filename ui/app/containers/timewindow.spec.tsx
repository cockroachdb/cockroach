import * as React from "react";
import { assert } from "chai";
import { shallow } from "enzyme";
import * as sinon from "sinon";
import moment = require("moment");
import _ = require("lodash");

import { TimeWindowManagerUnconnected as TimeWindowManager } from "./timewindow";
import * as timewindow from "../redux/timewindow";

describe("<TimeWindowManager>", function() {
  let spy: sinon.SinonSpy;
  let state: timewindow.TimeWindowState;
  let now = () => moment("11-12-1955 10:04PM -0800", "MM-DD-YYYY hh:mma Z");

  beforeEach(function() {
    spy = sinon.spy();
    state = new timewindow.TimeWindowState();
  });

  let getManager = () => shallow(<TimeWindowManager timeWindow={_.clone(state)}
                                                    setTimeWindow={spy}
                                                    now={now} />);

  it("resets time window immediately it is empty", function() {
    getManager();
    assert.isTrue(spy.calledOnce);
    assert.deepEqual(spy.firstCall.args[0], {
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("resets time window immediately if expired", function() {
    state.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      end: now().subtract(state.scale.windowValid).subtract(1),
    };

    getManager();
    assert.isTrue(spy.calledOnce);
    assert.deepEqual(spy.firstCall.args[0], {
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("resets time window immediately if scale has changed", function() {
    // valid window.
    state.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      end: now(),
    };
    state.scaleChanged = true;

    getManager();
    assert.isTrue(spy.calledOnce);
    assert.deepEqual(spy.firstCall.args[0], {
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("resets time window later if current window is valid", function() {
    state.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      // 5 milliseconds until expiration.
      end: now().subtract(state.scale.windowValid.asMilliseconds() - 5),
    };

    getManager();
    assert.isTrue(spy.notCalled);

    // Wait 11 milliseconds, then verify that window was updated.
    return new Promise<void>((resolve, reject) => {
      setTimeout(
        () => {
          assert.isTrue(spy.calledOnce);
          assert.deepEqual(spy.firstCall.args[0], {
            start: now().subtract(state.scale.windowSize),
            end: now(),
          });
          resolve();
        },
        6);
    });
  });

  it("has only a single timeout at a time.", function() {
    state.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      // 5 milliseconds until expiration.
      end: now().subtract(state.scale.windowValid.asMilliseconds() - 5),
    };

    let manager = getManager();
    assert.isTrue(spy.notCalled);

    // Set new props on currentWindow. The previous timeout should be abandoned.
    state.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      // 10 milliseconds until expiration.
      end: now().subtract(state.scale.windowValid.asMilliseconds() - 10),
    };
    manager.setProps({
      timeWindow: state,
    });
    assert.isTrue(spy.notCalled);

    // Wait 11 milliseconds, then verify that window was updated a single time.
    return new Promise<void>((resolve, reject) => {
      setTimeout(
        () => {
          assert.isTrue(spy.calledOnce);
          assert.deepEqual(spy.firstCall.args[0], {
            start: now().subtract(state.scale.windowSize),
            end: now(),
          });
          resolve();
        },
        11);
    });

  });
});
