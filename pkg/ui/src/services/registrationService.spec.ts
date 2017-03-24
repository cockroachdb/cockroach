import _ from "lodash";
import { assert } from "chai";
import { spy, SinonSpy } from "sinon";
import { Store } from "redux";
import * as protobuf from "protobufjs/minimal";

import registrationSyncListener from "./registrationService";
import * as registrationService from "./registrationService";
import * as protos from "../js/protos";
import * as api from "../util/api";
import { AdminUIState, createAdminUIStore } from "../redux/state";
import { KEY_REGISTRATION_SYNCHRONIZED, KEY_HELPUS, beginLoadUIData, beginSaveUIData, setUIDataKey } from "../redux/uiData";
import { clusterReducerObj } from "../redux/apiReducers";
import { COCKROACHLABS_ADDR } from "../util/cockroachlabsAPI";
import fetchMock from "../util/fetch-mock";

const CLUSTER_ID = "10101";
const uiDataPostFetchURL = `${api.API_PREFIX}/uidata`;
const uiDataFetchURL = `${api.API_PREFIX}/uidata?keys=registration_synchronized&keys=helpus`;
const clusterFetchURL = `${api.API_PREFIX}/cluster`;
const registrationFetchURLPrefixMatcher = `begin:${COCKROACHLABS_ADDR}`;
const unregistrationFetchURL = `${COCKROACHLABS_ADDR}/api/clusters/unregister?uuid=${CLUSTER_ID}`;
const registrationFetchURL = `${COCKROACHLABS_ADDR}/api/clusters/register?uuid=${CLUSTER_ID}`;

let listener: SinonSpy;
let store: Store<AdminUIState>;

// Generates a GetUIDataResponse with the given key value pairs.
function generateGetUIDataResponse(kvPairs: { [key: string]: any; }) {
  let values = _.mapValues(kvPairs, (v) => {
    const stringifiedValue = JSON.stringify(v);
    const buffer = new Uint8Array(protobuf.util.utf8.length(stringifiedValue));
    protobuf.util.utf8.write(stringifiedValue, buffer, 0);

    return {
      value: buffer,
      // // last_updated currently isn't used.
      // last_updated: { sec: "1476990411", nsec: 939569000 },
    };
  });
  const encodedResponse = protos.cockroach.server.serverpb.GetUIDataResponse.encode({ key_values: values }).finish();
  return api.toArrayBuffer(encodedResponse);
}

describe("registration service helper functions", function () {
  function stateHelperReset() {
    store = createAdminUIStore();
    registrationService.setSaving(false);
    registrationService.resetErrors();
  }

  beforeEach(function () {
    stateHelperReset();
  });

  it("should use helper functions to set/get saving and error state", function () {
    assert.isFalse(registrationService.getSaving());
    registrationService.setSaving(true);
    assert.isTrue(registrationService.getSaving());
    assert.equal(registrationService.getErrors(), 0);
    registrationService.incrErrors();
    assert.equal(registrationService.getErrors(), 1);
    registrationService.incrErrors();
    assert.equal(registrationService.getErrors(), 2);
    registrationService.resetErrors();
    assert.equal(registrationService.getErrors(), 0);
  });

  describe("shouldRun", function () {
    it("should run by default", function () {
      assert(registrationService.shouldRun(store.getState()));
    });
    it("shouldn't run if any data is in flight", function () {
      store.dispatch(beginLoadUIData([KEY_HELPUS]));
      assert.isFalse(registrationService.shouldRun(store.getState()));
      store.dispatch(beginSaveUIData([KEY_HELPUS]));
      assert.isFalse(registrationService.shouldRun(store.getState()));

      store.dispatch(beginLoadUIData([KEY_REGISTRATION_SYNCHRONIZED]));
      assert.isFalse(registrationService.shouldRun(store.getState()));
      store.dispatch(beginSaveUIData([KEY_REGISTRATION_SYNCHRONIZED]));
      assert.isFalse(registrationService.shouldRun(store.getState()));
    });

    it("shouldn't run if any data is saving", function () {
      registrationService.setSaving(true);
      assert.isFalse(registrationService.shouldRun(store.getState()));
    });

    it("shouldn't run if there are too many errors", function () {
      _.times(registrationService.ERROR_LIMIT, registrationService.incrErrors);
      assert.isFalse(registrationService.shouldRun(store.getState()));
    });
  });

  describe("shouldLoadData", function () {
    it("should return true if all data is missing", function () {
      assert(registrationService.shouldLoadKeys(store.getState()));
      assert(registrationService.shouldLoadClusterInfo(store.getState()));
      assert(registrationService.shouldLoadData(store.getState()));
    });

    it("should return true if cluster info is missing", function () {
      store.dispatch(setUIDataKey(KEY_HELPUS, {}));
      store.dispatch(setUIDataKey(KEY_REGISTRATION_SYNCHRONIZED, true));
      assert.isFalse(registrationService.shouldLoadKeys(store.getState()));
      assert(registrationService.shouldLoadClusterInfo(store.getState()));
      assert(registrationService.shouldLoadData(store.getState()));
    });

    it("should return true if uiData keys are missing", function () {
      store.dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({cluster_id: CLUSTER_ID})));
      assert(registrationService.shouldLoadKeys(store.getState()));
      assert.isFalse(registrationService.shouldLoadClusterInfo(store.getState()));
      assert(registrationService.shouldLoadData(store.getState()));
    });

    it("should return false if no data is missing", function () {
      store.dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({cluster_id: CLUSTER_ID})));
      store.dispatch(setUIDataKey(KEY_HELPUS, {}));
      store.dispatch(setUIDataKey(KEY_REGISTRATION_SYNCHRONIZED, true));
      assert.isFalse(registrationService.shouldLoadKeys(store.getState()));
      assert.isFalse(registrationService.shouldLoadClusterInfo(store.getState()));
      assert.isFalse(registrationService.shouldLoadData(store.getState()));
    });
  });

  describe("loadNeededData", function () {
    let dispatch: SinonSpy;

    beforeEach(function () {
      dispatch = spy(store.dispatch);
    });

    it("should load all data by default", function () {
      registrationService.loadNeededData(store.getState(), dispatch);
      assert.equal(dispatch.callCount, 2);
      assert.isFunction(dispatch.args[0][0]);
    });

    it("should load keys if keys are missing", function () {
      store.dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({ cluster_id: CLUSTER_ID })));
      registrationService.loadNeededData(store.getState(), dispatch);
      assert.equal(dispatch.callCount, 1);
      assert.isFunction(dispatch.args[0][0]);
    });

    it("should load cluster id if cluster id is missing", function () {
      store.dispatch(setUIDataKey(KEY_HELPUS, {}));
      store.dispatch(setUIDataKey(KEY_REGISTRATION_SYNCHRONIZED, true));
      registrationService.loadNeededData(store.getState(), dispatch);
      assert.equal(dispatch.callCount, 1);
      assert.isFunction(dispatch.args[0][0]);
    });

    it("should load nothing if no data is missing", function () {
      store.dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({cluster_id: CLUSTER_ID})));
      store.dispatch(setUIDataKey(KEY_HELPUS, {}));
      store.dispatch(setUIDataKey(KEY_REGISTRATION_SYNCHRONIZED, true));
      registrationService.loadNeededData(store.getState(), dispatch);
      assert.isFalse(dispatch.called);
    });
  });

  describe("syncRegistration", function () {
    let dispatch: (a: any) => void;

    beforeEach(function () {
      dispatch = store.dispatch;
    });

    afterEach(fetchMock.restore);

    it("unregisters if optin is false", function (done) {
      fetchMock.mock({
        matcher: unregistrationFetchURL,
        response: () => {
          assert(registrationService.getSaving());
          assert.equal(registrationService.getErrors(), 0);
          return "{}";
        },
      });

      fetchMock.mock({
        matcher: uiDataPostFetchURL,
        response: (_url: string, requestObj: RequestInit) => {
          assert(registrationService.getSaving());
          assert.equal(registrationService.getErrors(), 0);
          const uiDataRequest = protos.cockroach.server.serverpb.SetUIDataRequest.decode(new Uint8Array(requestObj.body as ArrayBuffer));
          const buffer = uiDataRequest.key_values[KEY_REGISTRATION_SYNCHRONIZED];
          assert.equal(JSON.parse(protobuf.util.utf8.read(buffer, 0, buffer.byteLength)), true);
          return 200;
        },
      });

      fetchMock.mock({
        matcher: registrationFetchURL,
        response: () => {
          done(new Error("Should not have tried to register the cluster."));
        },
      });

      dispatch(setUIDataKey(KEY_HELPUS, { optin: false }));
      dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({cluster_id: CLUSTER_ID})));
      registrationService.syncRegistration(store.getState(), dispatch, store.getState).then(() => {
        assert.isFalse(registrationService.getSaving());
        assert.equal(registrationService.getErrors(), 0);
        assert.lengthOf(fetchMock.calls(unregistrationFetchURL), 1);
        assert.lengthOf(fetchMock.calls(uiDataPostFetchURL), 1);
        assert.lengthOf(fetchMock.calls(registrationFetchURL), 0);
        done();
      });
    });

    it("registers if optin is true", function (done) {
      fetchMock.mock({
        matcher: registrationFetchURL,
        response: () => {
          assert(registrationService.getSaving());
          assert.equal(registrationService.getErrors(), 0);
          return "{}";
        },
      });

      fetchMock.mock({
        matcher: uiDataPostFetchURL,
        response: (_url: string, requestObj: RequestInit) => {
          assert(registrationService.getSaving());
          assert.equal(registrationService.getErrors(), 0);
          const uiDataRequest = protos.cockroach.server.serverpb.SetUIDataRequest.decode(new Uint8Array(requestObj.body as ArrayBuffer));
          const buffer = uiDataRequest.key_values[KEY_REGISTRATION_SYNCHRONIZED];
          assert.equal(JSON.parse(protobuf.util.utf8.read(buffer, 0, buffer.byteLength)), true);
          return 200;
        },
      });

      fetchMock.mock({
        matcher: unregistrationFetchURL,
        response: () => {
          done(new Error("Should not have tried to unregister the cluster."));
        },
      });

      dispatch(setUIDataKey(KEY_HELPUS, { optin: true }));
      dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({cluster_id: CLUSTER_ID})));
      registrationService.syncRegistration(store.getState(), dispatch, store.getState).then(() => {
        assert.isFalse(registrationService.getSaving());
        assert.equal(registrationService.getErrors(), 0);
        assert.lengthOf(fetchMock.calls(unregistrationFetchURL), 0);
        assert.lengthOf(fetchMock.calls(uiDataPostFetchURL), 1);
        assert.lengthOf(fetchMock.calls(registrationFetchURL), 1);
        done();
      });
    });

    it("tracks errors", function (done) {
      fetchMock.mock({
        matcher: registrationFetchURL,
        response: () => {
          assert(registrationService.getSaving());
          assert.equal(registrationService.getErrors(), 0);
          return { throws: new Error() };
        },
      });

      fetchMock.mock({
        matcher: uiDataPostFetchURL,
        response: (_url, _req) => {
          done(new Error("Should not have tried to set uiData."));

        },
      });

      fetchMock.mock({
        matcher: unregistrationFetchURL,
        response: () => {
          done(new Error("Should not have tried to unregister the cluster."));
        },
      });

      dispatch(setUIDataKey(KEY_HELPUS, { optin: true }));
      dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({cluster_id: CLUSTER_ID})));
      registrationService.syncRegistration(store.getState(), dispatch, store.getState).then(() => {
        assert.isFalse(registrationService.getSaving());
        assert.equal(registrationService.getErrors(), 1);
        assert.lengthOf(fetchMock.calls(unregistrationFetchURL), 0);
        assert.lengthOf(fetchMock.calls(uiDataPostFetchURL), 0);
        assert.lengthOf(fetchMock.calls(registrationFetchURL), 1);
        done();
      });
    });
  });
});

describe("registration sync end to end", function() {
  beforeEach(function () {
    store = createAdminUIStore();
    listener = spy(registrationSyncListener(store));
    store.subscribe(listener);
  });

  afterEach(fetchMock.restore);

  it("doesn't sync if the uiData registration_synchronized value is true", function (done) {
    assert(listener.notCalled);

    fetchMock.mock({
      matcher: uiDataFetchURL,
      response: () => {
        let body = generateGetUIDataResponse({
          [KEY_REGISTRATION_SYNCHRONIZED]: true,
        });
        return { body };
      },
    });

    fetchMock.mock({
      matcher: clusterFetchURL,
      response: () => {
        const encodedResponse = protos.cockroach.server.serverpb.ClusterResponse.encode({ cluster_id: CLUSTER_ID }).finish();
        return {
          body: api.toArrayBuffer(encodedResponse),
        };
      },
    });

    fetchMock.mock({
      matcher: registrationFetchURLPrefixMatcher,
      response: () => {
        done(new Error("Should not have tried to contact the registration server."));
      },
    });

    // Trigger the store subscription.
    store.dispatch({ type: null });
    assert(listener.called);

    // Watch the state and complete the test successfully if we last a tick with
    // the expected information.
    let timeout: number;
    store.subscribe(() => {
      let state = store.getState();
      clearTimeout(timeout);
      if (state.cachedData.cluster.data &&
        state.cachedData.cluster.data.cluster_id &&
        state.uiData[KEY_REGISTRATION_SYNCHRONIZED] &&
        state.uiData[KEY_REGISTRATION_SYNCHRONIZED].data
      ) {
        assert.lengthOf(fetchMock.calls(uiDataFetchURL), 1);
        assert.lengthOf(fetchMock.calls(clusterFetchURL), 1);
        assert.lengthOf(fetchMock.calls(registrationFetchURLPrefixMatcher), 0);
        timeout = setTimeout(() => done());
      }
    });
  });

  it("attempts to unregister the cluster if optin is false", function (done) {
    assert(listener.notCalled);

    fetchMock.mock({
      matcher: uiDataFetchURL,
      response: () => {
        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });

        let body = generateGetUIDataResponse({
          [KEY_REGISTRATION_SYNCHRONIZED]: false,
        });
        return { body };
      },
    });

    fetchMock.mock({
      matcher: uiDataPostFetchURL,
      response: (_url, _req) => {
        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });
        return 200;
      },
    });

    fetchMock.mock({
      matcher: clusterFetchURL,
      response: () => {
        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });
        const encodedResponse = protos.cockroach.server.serverpb.ClusterResponse.encode({ cluster_id: CLUSTER_ID }).finish();
        return {
          body: api.toArrayBuffer(encodedResponse),
        };
      },
    });

    fetchMock.mock({
      matcher: unregistrationFetchURL,
      response: (_url, _req) => {
        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });
        return "{}";
      },
    });
    // Trigger the store subscription.
    store.dispatch({ type: null });

    // Watch the state and complete the test successfully if we last a tick with
    // the expected information.
    let timeout: number;
    store.subscribe(() => {
      let state = store.getState();
      clearTimeout(timeout);
      if (state.cachedData.cluster.data &&
        state.cachedData.cluster.data.cluster_id &&
        state.uiData[KEY_REGISTRATION_SYNCHRONIZED] &&
        state.uiData[KEY_REGISTRATION_SYNCHRONIZED].data
      ) {
        // Ensure every relevant url is called exactly once.
        assert.lengthOf(fetchMock.calls(uiDataFetchURL), 1);
        assert.lengthOf(fetchMock.calls(uiDataPostFetchURL), 1);
        assert.lengthOf(fetchMock.calls(clusterFetchURL), 1);
        assert.lengthOf(fetchMock.calls(registrationFetchURL), 0);
        assert.lengthOf(fetchMock.calls(unregistrationFetchURL), 1);
        timeout = setTimeout(() => done());
      }
    });
  });

  it("attempts to register the cluster if optin is true", function (done) {
    assert(listener.notCalled);

    fetchMock.mock({
      matcher: uiDataFetchURL,
      response: () => {
        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });

        let body = generateGetUIDataResponse({
          [KEY_REGISTRATION_SYNCHRONIZED]: false,
          [KEY_HELPUS]: {optin: true},
        });
        return { body };
      },
    });

    fetchMock.mock({
      matcher: uiDataPostFetchURL,
      response: (_url, _req) => {
        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });
        return 200;
      },
    });

    fetchMock.mock({
      matcher: clusterFetchURL,
      response: () => {
        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });
        const encodedResponse = protos.cockroach.server.serverpb.ClusterResponse.encode({ cluster_id: CLUSTER_ID }).finish();
        return {
          body: api.toArrayBuffer(encodedResponse),
        };
      },
    });

    fetchMock.mock({
      matcher: registrationFetchURL,
      response: (_url, _req) => {
        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });
        return "{}";
      },
    });
    // Trigger the store subscription.
    store.dispatch({ type: null });

    // Watch the state and complete the test successfully if we last a tick with
    // the expected information.
    let timeout: number;
    store.subscribe(() => {
      let state = store.getState();
      clearTimeout(timeout);
      if (state.cachedData.cluster.data &&
        state.cachedData.cluster.data.cluster_id &&
        state.uiData[KEY_REGISTRATION_SYNCHRONIZED] &&
        state.uiData[KEY_REGISTRATION_SYNCHRONIZED].data
      ) {
        // Ensure every relevant url is called exactly once.
        assert.lengthOf(fetchMock.calls(uiDataFetchURL), 1);
        assert.lengthOf(fetchMock.calls(uiDataPostFetchURL), 1);
        assert.lengthOf(fetchMock.calls(clusterFetchURL), 1);
        assert.lengthOf(fetchMock.calls(registrationFetchURL), 1);
        assert.lengthOf(fetchMock.calls(unregistrationFetchURL), 0);
        timeout = setTimeout(() => done());
      }
    });
  });
});
