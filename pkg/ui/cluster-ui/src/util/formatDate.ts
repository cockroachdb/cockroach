import { Moment } from "moment";

export const dateFormat = "Y-MM-DD HH:mm:ss";

export function formatDate(time: Moment) {
  return time.format(dateFormat);
}
