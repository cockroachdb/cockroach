export interface DataFromServer {
  ExperimentalUseLogin: boolean;
  LoginEnabled: boolean;
  LoggedInUser: string;
  Tag: string;
  Version: string;
}

// Tell TypeScript about `window.dataFromServer`, which is set in a script
// tag in index.html, the contents of which are generated in a Go template
// server-side.
declare global {
  interface Window {
    dataFromServer: DataFromServer;
  }
}

export function getDataFromServer(): DataFromServer {
  return window.dataFromServer || {} as DataFromServer;
}
