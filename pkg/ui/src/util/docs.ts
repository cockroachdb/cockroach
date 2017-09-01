// TODO(benesch): Derive this URL from build.VersionPrefix() rather than
// hardcoding v1.1. This is harder than it sounds, since we don't want to encode
// the branch name in embedded.go.
const docsURLBase = "https://www.cockroachlabs.com/docs/v1.1";

export default function docsURL(pageName: string): string {
  return `${docsURLBase}/${pageName}`;
}
