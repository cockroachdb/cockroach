// TODO(benesch): upstream this.

declare module "rc-progress" {
  export interface LineProps {
    strokeWidth?: number;
    trailWidth?: number;
    className?: string;
    percent?: number;
  }
  export class Line extends React.Component<LineProps, {}> {
  }
}
