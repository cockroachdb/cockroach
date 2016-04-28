import { assert } from "chai";
import _ = require("lodash");
import * as fetchMock from "fetch-mock";

import {getDatabaseList, API_PREFIX, setFetchTimeout} from "./api";

type DatabasesResponse = cockroach.server.DatabasesResponse;

describe("rest api", function() {
  describe("databases request", function () {
    afterEach(function () {
      fetchMock.restore();
    });

    it("correctly requests info about all databases", function () {
      this.timeout(1000);
      // Mock out the fetch query to /databases
      fetchMock.mock(API_PREFIX + "/databases", "get", (url: string, requestObj: RequestInit) => {

        assert.isUndefined(requestObj.body);

        return {"databases": ["system", "test"]};
      });

      return getDatabaseList().then(function (result: DatabasesResponse) {
        assert.lengthOf(fetchMock.calls(API_PREFIX + "/databases"), 1);
        assert.lengthOf(result.databases, 2);
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      setFetchTimeout(0);
      // Mock out the fetch query to /databases, but return a promise that's never resolved to test the timeout
      fetchMock.mock(API_PREFIX + "/databases", "get", (url: string, requestObj: RequestInit) => {
        assert.isUndefined(requestObj.body);
        return new Promise<any>(() => { });
      });

      getDatabaseList().then(function (result: DatabasesResponse) {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.startsWith(e.message, "Promise timed out"), "Error is a timeout error.");
        done();
      });
    });

  });
});
