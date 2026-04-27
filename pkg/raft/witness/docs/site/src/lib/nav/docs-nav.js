// Sidebar nav config for the (docs) route group.
//
// `pages` items have a `type` of:
//   - 'page'    — top-level page link
//   - 'section' — section header that is itself a page
//   - 'sub'     — sub-item under the preceding section header

export const home = { label: 'Raft Witness', href: '/' };

export const crossLinks = [
  // Add cross-links here when there are multiple route groups, e.g.
  // { label: 'Code Guide', href: '/code-guide' },
];

export const pages = [
  { type: 'page', href: '/introduction',    label: 'Introduction' },
  { type: 'page', href: '/cost-details',    label: 'Cost of raft replication' }
];
