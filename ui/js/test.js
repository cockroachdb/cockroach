/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/chai/chai.d.ts" />
suite("Javascript Test Framework", function () {
    var assert = chai.assert;
    test("assertions work.", function () {
        assert.equal("foo", "foo", "equality assertion didn't work.");
    });
});
