import { getDataFromServer } from "src/util/dataFromServer";

const version = getDataFromServer().Version || "stable";
const docsURLBase = "https://www.cockroachlabs.com/docs/" + version;

export default function docsURL(pageName: string): string {
  return `${docsURLBase}/${pageName}`;
}
