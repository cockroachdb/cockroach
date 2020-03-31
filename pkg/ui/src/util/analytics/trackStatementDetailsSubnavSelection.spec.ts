import { get, isString } from "lodash";
import { assert } from "chai";
import { createSandbox } from "sinon";
import { track } from "./trackStatementDetailsSubnavSelection";

const sandbox = createSandbox();

describe("trackSubnavSelection", () => {
  afterEach(() => {
    sandbox.reset();
  });

  it("should send a track call given a selected nav item key", () => {
    const spy = sandbox.spy();
    const subNavKey = "subnav-test";

    track(spy)(subNavKey);

    const sent = spy.getCall(0).args[0];
    const selection = get(sent, "properties.selection");

    assert.isTrue(spy.calledOnce);
    assert.isTrue(isString(selection));
    assert.isTrue(selection === subNavKey);
  });
});
