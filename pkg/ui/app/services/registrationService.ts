import _ from "lodash";
import { Store } from "redux";

import { registerCluster, unregisterCluster } from "../util/cockroachlabsAPI";

import { AdminUIState } from "../redux/state";
import * as uiData from "../redux/uiData";
import { refreshCluster } from "../redux/apiReducers";

export const ERROR_LIMIT = 3;
export const neededKeys = [uiData.KEY_REGISTRATION_SYNCHRONIZED, uiData.KEY_HELPUS];

let saving = false;
let errors = 0;

export function setSaving(s: boolean) {
  saving = s;
}

export function getSaving() {
  return saving;
}

export function resetErrors() {
  errors = 0;
}

export function incrErrors() {
  errors++;
}

export function getErrors() {
  return errors;
}

// Automatically return if any uiData is being loaded from or saved to the
// backend, if the Cockroach Labs server is already being contacted, or if
// the number of errors seen from trying to contact the Cockroach Labs
// server is at the limit.
export function shouldRun(state: AdminUIState): boolean {
  return !uiData.isInFlight(state, uiData.KEY_HELPUS) &&
    !uiData.isInFlight(state, uiData.KEY_REGISTRATION_SYNCHRONIZED) &&
    !getSaving() &&
    getErrors() < ERROR_LIMIT;
}

// Returns false if all uidata keys have been retreived from the backend and
// true otherwise.
export function shouldLoadKeys(state: AdminUIState): boolean {
  return _.some(neededKeys, (key) => !uiData.isValid(state, key));
}

// Returns false if cluster info has been retreived from the backend and true
// otherwise.
export function shouldLoadClusterInfo(state: AdminUIState): boolean {
  return !state.cachedData.cluster.valid || !state.cachedData.cluster.data || !state.cachedData.cluster.data.cluster_id;
}

// Returns false if uiData and cluster info have been retreived, true otherwise.
export function shouldLoadData(state: AdminUIState): boolean {
  return shouldLoadKeys(state) || shouldLoadClusterInfo(state);
}

// Ensure all necessary uiData keys have been retrieved from the backend and
// ensure clusterID has been retreived.
export function loadNeededData(state: AdminUIState, dispatch: (a: any) => any) {
  if (shouldLoadKeys(state)) {
    dispatch(uiData.loadUIData(...neededKeys));
  }

  if (shouldLoadClusterInfo(state)) {
    dispatch(refreshCluster());
  }
}

// Returns true if the registration has not been synced.
export function shouldSyncRegistration(state: AdminUIState): boolean {
  return !uiData.getData(state, uiData.KEY_REGISTRATION_SYNCHRONIZED);
}

export function registrationDidSync(oldState: AdminUIState, newState: AdminUIState) {
  return _.isEqual(uiData.getData(oldState, uiData.KEY_HELPUS), uiData.getData(newState, uiData.KEY_HELPUS));
}

export function saveRegistrationSynced(dispatch: (a: any) => any) {
  dispatch(uiData.saveUIData({ key: uiData.KEY_REGISTRATION_SYNCHRONIZED, value: true }));
}

export function syncRegistration(state: AdminUIState, dispatch: (a: any) => any, getState: () => AdminUIState) {
  const registrationCallback = () => {
    // Reset the error count.
    resetErrors();
    if (registrationDidSync(state, getState())) {
      saveRegistrationSynced(dispatch);
    }
  };

  setSaving(true);

  const helpusData: uiData.OptInAttributes = uiData.getData(state, uiData.KEY_HELPUS);
  const clusterId = state.cachedData.cluster.data.cluster_id;
  let cockroachServerRequest: Promise<{}>;
  // Register the cluster if the user opted in. Deregister it if they opted out.
  if (helpusData && helpusData.optin) {
    cockroachServerRequest = registerCluster({
      first_name: helpusData.firstname,
      last_name: helpusData.lastname,
      company: helpusData.company,
      email: helpusData.email,
      clusterID: clusterId,
      product_updates: helpusData.updates,
    });
  } else {
    cockroachServerRequest = unregisterCluster({
      clusterID: clusterId,
    });
  }

  return cockroachServerRequest
    .then(registrationCallback)
    .catch((_e) => incrErrors())
    .then(() => setSaving(false));
}

/**
 *  This function, when passed in a redux store, generates a service which keeps
 *  the user's registration data synchronized with the Cockroach Labs servers.
 */
export default function (store: Store<AdminUIState>) {
  return () => {
    let dispatch: (a: any) => any = store.dispatch;
    let state: AdminUIState = store.getState();

    if (!shouldRun(state)) { return; }

    if (shouldLoadData(state)) {
      loadNeededData(state, dispatch);
      // We don't have all the data loaded. We need to wait until the state is
      // updated with all the data we need.
      return;
    }

    // If registration is synchronized, don't continue.
    if (!shouldSyncRegistration(state)) { return; }

    // Registration isn't synchronized, so attempt to persist it to the Cockroach
    // Labs server.
    syncRegistration(state, dispatch, store.getState);
  };
}
