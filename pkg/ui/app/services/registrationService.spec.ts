import { assert } from "chai";
import { spy, SinonSpy } from "sinon";
import { Store } from "redux";

import registrationSyncListener from "./registrationService";
import * as protos from "../js/protos";
import { AdminUIState, createAdminUIStore } from "../redux/state";
import { setUIDataKey, KEY_REGISTRATION_SYNCHRONIZED, KEY_HELPUS, beginLoadUIData, completeLoadUIData } from "../redux/uiData";
import { COCKROACHLABS_ADDR } from "../util/cockroachlabsAPI";
import fetchMock from "../util/fetch-mock";

const CLUSTER_ID = "10101";
const uiDataPostFetchURL = "/_admin/v1/uidata";
const uiDataFetchURL = "/_admin/v1/uidata?keys=registration_synchronized&keys=helpus";
const clusterFetchURL = "/_admin/v1/cluster";
const registrationFetchURLPrefix = `^${COCKROACHLABS_ADDR}`;
const unregistrationFetchURL = `${COCKROACHLABS_ADDR}/api/clusters/unregister?uuid=${CLUSTER_ID}`;
const registrationFetchURL = `${COCKROACHLABS_ADDR}/api/clusters/register?uuid=${CLUSTER_ID}`;

let listener: SinonSpy;
let store: Store<AdminUIState>;

let keys = [KEY_REGISTRATION_SYNCHRONIZED, KEY_HELPUS];

describe("registration sync", function() {
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
        setTimeout(() => {
          store.dispatch(beginLoadUIData(keys));
          store.dispatch(setUIDataKey(KEY_REGISTRATION_SYNCHRONIZED, true));
          store.dispatch(setUIDataKey(KEY_HELPUS, undefined));
          store.dispatch(completeLoadUIData(keys));
        });

        // HACK: Return an error so that the values don't get set by the uiData
        // actionCreator callback. Instead we set them directly above.
        // TODO(maxlang): It might be useful to mock uiData responses.
        return { throws: new Error() };
      },
    });

    fetchMock.mock({
      matcher: clusterFetchURL,
      response: () => {
        return {
          body: (new protos.cockroach.server.serverpb.ClusterResponse({ cluster_id: CLUSTER_ID })).toArrayBuffer(),
        };
      },
    });

    fetchMock.mock({
      matcher: registrationFetchURLPrefix,
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
        assert.lengthOf(fetchMock.calls(registrationFetchURLPrefix), 0);
        timeout = setTimeout(() => done());
      }
    });
  });

  it("attempts to unregister the cluster if optin is false", function (done) {
    assert(listener.notCalled);

    fetchMock.mock({
      matcher: uiDataFetchURL,
      response: () => {
        setTimeout(() => {
          store.dispatch(beginLoadUIData(keys));
          store.dispatch(setUIDataKey(KEY_REGISTRATION_SYNCHRONIZED, false));
          store.dispatch(setUIDataKey(KEY_HELPUS, undefined));
          store.dispatch(completeLoadUIData(keys));
        });

        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });

        // HACK: Return an error so that the values don't get set by the uiData
        // actionCreator callback. Instead we set them directly above.
        // TODO(maxlang): It might be useful to mock uiData responses.
        return { throws: new Error() };
      },
    });

    fetchMock.mock({
      matcher: uiDataPostFetchURL,
      response: (url, req) => {
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
        return {
          body: (new protos.cockroach.server.serverpb.ClusterResponse({ cluster_id: CLUSTER_ID })).toArrayBuffer(),
        };
      },
    });

    fetchMock.mock({
      matcher: unregistrationFetchURL,
      response: (url, req) => {
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
        setTimeout(() => {
          store.dispatch(beginLoadUIData(keys));
          store.dispatch(setUIDataKey(KEY_REGISTRATION_SYNCHRONIZED, false));
          store.dispatch(setUIDataKey(KEY_HELPUS, {optin: true}));
          store.dispatch(completeLoadUIData(keys));
        });

        // This dispatch will trigger the listener, but it shouldn't trigger any
        // other requests.
        store.dispatch({ type: null });

        // HACK: Return an error so that the values don't get set by the uiData
        // actionCreator callback. Instead we set them directly above.
        // TODO(maxlang): It might be useful to mock uiData responses.
        return { throws: new Error() };
      },
    });

    fetchMock.mock({
      matcher: uiDataPostFetchURL,
      response: (url, req) => {
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
        return {
          body: (new protos.cockroach.server.serverpb.ClusterResponse({ cluster_id: CLUSTER_ID })).toArrayBuffer(),
        };
      },
    });

    fetchMock.mock({
      matcher: registrationFetchURL,
      response: (url, req) => {
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
