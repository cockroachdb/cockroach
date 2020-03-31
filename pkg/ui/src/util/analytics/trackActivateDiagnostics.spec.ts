import { get, isString } from "lodash";
import { assert } from "chai";
import { createSandbox } from "sinon";
import { track } from "./trackActivateDiagnostics";

const sandbox = createSandbox();

describe("trackActivateDiagnostics", () => {
  afterEach(() => {
    sandbox.reset();
  });

  it("should send a track call given a payload", () => {
    const spy = sandbox.spy();
    const statement = "SELECT blah from blah-blah";

    track(spy)(statement);

    const sent = spy.getCall(0).args[0];
    const fingerprint = get(sent, "properties.fingerprint");

    assert.isTrue(spy.calledOnce);
    assert.isTrue(isString(fingerprint));
    assert.isTrue( fingerprint === statement);
  });
});
