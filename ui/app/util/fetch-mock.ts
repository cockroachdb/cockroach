import "../js/object-assign";
import * as fetchMock from "fetch-mock";

fetchMock.configure({
  sendAsJson: false,
});

export = fetchMock;
