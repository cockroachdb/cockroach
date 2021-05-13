import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { DOMAIN_NAME } from "../utils";

export type LocalStorageState = {
  "adminUi/showDiagnosticsModal": boolean;
  "showColumns/StatementsPage": string;
};

type Payload = {
  key: keyof LocalStorageState;
  value: any;
};

// TODO (koorosh): initial state should be restored from preserved keys in LocalStorage
const initialState: LocalStorageState = {
  "adminUi/showDiagnosticsModal":
    Boolean(localStorage.getItem("adminUi/showDiagnosticsModal")) || false,
  "showColumns/StatementsPage":
    localStorage.getItem("showColumns/StatementsPage") || "default",
};

const localStorageSlice = createSlice({
  name: `${DOMAIN_NAME}/localStorage`,
  initialState,
  reducers: {
    update: (state: any, action: PayloadAction<Payload>) => {
      state[action.payload.key] = action.payload.value;
    },
  },
});

export const { actions, reducer } = localStorageSlice;
