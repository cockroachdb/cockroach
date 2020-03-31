import { analytics } from "src/redux/analytics";

export const track = (fn: Function) => (selection: string) => {
  fn({
    event: "SubNavigation Selection",
    properties: {
      selection,
    },
  });
};

export default function trackSubnavSelection (selection: string) {
  const boundTrack = analytics.track.bind(analytics);
  track(boundTrack)(selection);
}
