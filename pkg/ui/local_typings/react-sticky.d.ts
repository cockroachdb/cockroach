import {ReactNode, Component} from 'react';

declare class StickyContainer extends Component<any, {}> {}

declare interface IStickyProps {
    className?: string,
    stickyClassName?: string,
    topOffset?: number,
    bottomOffset?: number,
    style?: any,
    stickyStyle?: any,
    onStickyStateChange?: () => void,
    isActive?: boolean,
}

declare class Sticky extends Component<IStickyProps, {}> {}