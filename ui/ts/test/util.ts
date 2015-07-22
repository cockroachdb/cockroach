/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/chai/chai.d.ts" />

suite("Javascript Test Framework", () => {
  let assert = chai.assert;
  test("assertions work.", () => {
    assert.equal("foo", "foo", "equality assertion didn't work.");
  });
});
