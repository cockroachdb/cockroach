import _ from "lodash";

import { registerCluster, unregisterCluster } from "../util/cockroachlabsAPI";

import { store } from "../redux/state";
import { AdminUIState } from "../redux/state";
import { loadUIData, saveUIData, KEY_HELPUS, KEY_REGISTRATION_SYNCHRONIZED, OptInAttributes } from "../redux/uiData";
import { refreshCluster } from "../redux/apiReducers";

const ERROR_LIMIT = 3;
const neededKeys = [KEY_REGISTRATION_SYNCHRONIZED, KEY_HELPUS];

let saving = false;
let errors = 0;

/**
 *  This service keeps the user's registration data synchronized with the Cockroach Labs servers.
 */
store.subscribe(function registrationSynchronizer() {
  let dispatch: (a: any) => any = store.dispatch;
  let state: AdminUIState = store.getState();

  // Automatically return if any uiData is being loaded from the backend, if the
  // Cockroach Labs server is already being contacted, or if the number of
  // errors seen from trying to contact the Cockroach Labs server is at the
  // limit.
  if (state.uiData.inFlight > 0 || saving || errors >= ERROR_LIMIT) {
    return;
  }

  // Track if a request has been initiated.
  let requesting = false;

  // Ensure all necessary uiData keys have been retrieved from the backend.
  if (_.some(neededKeys, (key) => !_.has(state.uiData.data, key))) {
    dispatch(loadUIData.apply(null, neededKeys));
    requesting = true;
  }

  // If registration is synchronized, don't continue.
  if (state.uiData.data[KEY_REGISTRATION_SYNCHRONIZED]) {
    return;
  }

  // Ensure clusterID has been retreived.
  if (!state.cachedData.cluster.valid || !state.cachedData.cluster.data || !state.cachedData.cluster.data.cluster_id) {
    dispatch(refreshCluster());
    requesting = true;
  }

  // We don't have all the data loaded. We need to wait until the state is
  // updated with all the data we need.
  if (requesting) {
    return;
  }

  // Registration isn't synchronized, so attempt to persist it to the Cockroach
  // Labs server.
  const helpusData: OptInAttributes = state.uiData.data[KEY_HELPUS];
  const registrationCallback = () => {
    // Reset the error count.
    errors = 0;
    // Refresh the state to ensure the data we saved is the same as the latest
    // data in the UI.
    state = store.getState();
    if (_.isEqual(state.uiData.data[KEY_HELPUS], helpusData)) {
      dispatch(saveUIData({ key: KEY_REGISTRATION_SYNCHRONIZED, value: true }));
    }
  };

  saving = true;

  // Register the cluster if the user opted in. Deregister it if they opted out.
  if (helpusData && helpusData.optin) {
    registerCluster({
      first_name: helpusData.firstname,
      last_name: helpusData.lastname,
      company: helpusData.company,
      email: helpusData.email,
      clusterID: state.cachedData.cluster.data.cluster_id,
    }).then(registrationCallback).catch(() => errors++).then(() => saving = false);
  } else {
    unregisterCluster({
      clusterID: state.cachedData.cluster.data.cluster_id,
    }).then(registrationCallback).catch(() => errors++).then(() => saving = false);
  }
});
