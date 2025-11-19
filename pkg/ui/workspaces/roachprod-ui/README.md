# Roachprod UI

Web-based user interface for managing roachprod clusters.

## Overview

The Roachprod UI provides a graphical interface for managing roachprod clusters, featuring:

- **Cluster List View**: Browse and filter all roachprod clusters
- **Split Panel Layout**: Cluster list on the left, SSH terminal on the right
- **Create Cluster Wizard**: Simple form with collapsible advanced options
- **Cluster Management**: Extend lifetimes, delete clusters
- **Interactive SSH Terminal**: Connect to cluster nodes via WebSocket

## Architecture

### Frontend (React + TypeScript)
- **Framework**: React 16.12.0 with TypeScript
- **UI Library**: Ant Design 5.6.1 (matching cluster-ui)
- **Terminal**: xterm.js with fit and web-links addons
- **Build Tool**: Webpack 5
- **Styling**: SCSS modules with design tokens from cluster-ui

### Backend (Go)
- **HTTP Server**: Embedded in roachprod binary
- **API**: RESTful JSON endpoints
- **SSH**: WebSocket-based terminal sessions
- **Integration**: Direct calls to roachprod library functions

### File Structure

```
pkg/ui/workspaces/roachprod-ui/
├── src/
│   ├── api/
│   │   └── roachprodApi.ts          # API client for backend
│   ├── components/
│   │   ├── CreateClusterModal/       # Cluster creation wizard
│   │   ├── ExtendClusterModal/       # Extend lifetime dialog
│   │   └── SSHTerminal/              # xterm.js terminal component
│   ├── pages/
│   │   └── ClustersPage/             # Main page with split layout
│   ├── styles/
│   │   ├── app.scss                  # Global styles
│   │   └── splitPanel.module.scss    # Split panel layout
│   ├── types/
│   │   └── cluster.ts                # TypeScript types
│   ├── App.tsx                       # Root component
│   └── index.tsx                     # Entry point
├── public/
│   └── index.html                    # HTML template
├── package.json
├── tsconfig.json
├── webpack.config.js
└── README.md

pkg/roachprod/ui/
├── server.go                         # HTTP server
├── handlers.go                       # REST API handlers
├── websocket.go                      # SSH WebSocket handler
├── types.go                          # Go types
└── dist/                             # Built frontend assets (generated)
```

## Development

### Prerequisites

- Node.js 16+ and pnpm
- Go 1.21+
- Roachprod binary built

### Setup

1. Install frontend dependencies:
```bash
cd pkg/ui
pnpm install
```

2. Build the UI:
```bash
cd pkg/ui/workspaces/roachprod-ui
pnpm run build
```

3. Build roachprod with embedded UI:
```bash
./dev build roachprod
```

### Development Workflow

#### Frontend Development (with hot reload)

1. Start the webpack dev server:
```bash
cd pkg/ui/workspaces/roachprod-ui
pnpm run dev
```

2. In another terminal, start the backend API server:
```bash
./bin/roachprod ui --port 7763
```

3. Access the UI at `http://localhost:3000`
   - Frontend dev server runs on port 3000
   - API requests are proxied to backend on port 7763
   - Hot reload enabled for instant feedback

#### Full Build

1. Build frontend assets:
```bash
cd pkg/ui/workspaces/roachprod-ui
pnpm run build
```

2. Build roachprod binary (embeds UI assets):
```bash
./dev build roachprod
```

3. Run roachprod UI:
```bash
./bin/roachprod ui
```

4. Access at `http://localhost:7763`

## Usage

### Starting the UI

```bash
# Default port (7763)
roachprod ui

# Custom port
roachprod ui --port 9000
```

### Features

#### Cluster List
- View all clusters or filter to show only your clusters
- Auto-refresh every 10 seconds
- Sortable columns (name, nodes, created date, etc.)
- Color-coded lifetime remaining (red for expired)

#### Create Cluster
1. Click "Create Cluster" button
2. Fill in basic options:
   - Cluster name (format: `<username>-<clustername>`)
   - Number of nodes
   - Cloud provider (GCE, AWS, Azure, Local)
   - Machine type
   - Lifetime in hours
   - Local SSD toggle
3. Optionally expand "Advanced Options":
   - Architecture (AMD64, ARM64, FIPS)
   - Geo-distributed toggle
   - Custom zones
   - Filesystem (ext4, ZFS)
4. Click "Create"

#### Extend Cluster
1. Click "Extend" button for a cluster
2. Enter extension duration (1-168 hours)
3. Click "Extend"

#### SSH Terminal
1. Click cluster name or "SSH" button
2. Terminal opens in right panel
3. Select node from dropdown (Node 1, 2, 3, etc.)
4. Interactive terminal session with the selected node
5. Connection status indicator (green=connected, yellow=connecting, red=disconnected)

#### Delete Cluster
1. Click "Delete" button for a cluster
2. Confirm deletion in popup dialog
3. Cluster is destroyed

## API Endpoints

### REST API

```
GET  /api/clusters              # List all clusters
GET  /api/clusters?user=<name>  # List user's clusters
POST /api/clusters/create       # Create new cluster
DELETE /api/clusters/delete     # Delete clusters
PUT  /api/clusters/:name/extend # Extend cluster lifetime
GET  /api/clusters/:name/details # Get cluster details
```

### WebSocket

```
WS /api/ssh/:cluster/:node      # SSH terminal session
```

## Configuration

### Frontend

Webpack config (`webpack.config.js`):
- Output directory: `../../roachprod/ui/dist`
- Dev server port: 3000
- API proxy: `http://localhost:7763`

### Backend

Server config (`pkg/roachprod/ui/server.go`):
- Default port: 7763
- Assets: Embedded from `dist/` directory
- CORS: Enabled for development
- Logging: Request logging middleware

## Troubleshooting

### UI assets not found

If you see "UI assets not built" message:

```bash
cd pkg/ui/workspaces/roachprod-ui
pnpm install
pnpm run build
```

Then rebuild roachprod:
```bash
./dev build roachprod
```

### WebSocket connection fails

1. Check that roachprod UI server is running
2. Verify port is correct (default 7763)
3. Check browser console for WebSocket errors
4. Ensure cluster name is valid

### Cluster creation fails

1. Verify cluster name format: `<username>-<clustername>`
2. Check cloud provider credentials are configured
3. Review roachprod logs for detailed error messages
4. Ensure required cloud CLI tools are installed (gcloud, aws, az)

### pnpm install fails

1. Ensure you're in the workspace root:
   ```bash
   cd pkg/ui
   pnpm install
   ```

2. If still failing, try:
   ```bash
   cd pkg/ui
   rm -rf node_modules pnpm-lock.yaml
   pnpm install
   ```

## Technology Stack

- **React 16.12.0**: UI framework
- **TypeScript 4.6.4**: Type-safe JavaScript
- **Ant Design 5.6.1**: UI component library
- **xterm.js 5.3.0**: Terminal emulator
- **Webpack 5**: Module bundler
- **SCSS**: CSS preprocessing
- **Go**: Backend server
- **WebSocket**: Real-time SSH communication

## Design Decisions

### Why React 16 instead of 18?
Matching the version used by cluster-ui for consistency and compatibility with the shared design system.

### Why Ant Design?
Already used by cluster-ui, provides consistent look and feel with the DB console, and includes comprehensive component library.

### Why embedded assets?
Single binary distribution - no need to deploy frontend separately. UI is embedded at build time using Go's `embed` directive.

### Why split panel layout?
Optimizes screen real estate for the two primary workflows:
- Browse/manage clusters (left panel)
- SSH into nodes (right panel)

Users can work with both views simultaneously without switching contexts.

## Future Enhancements

Potential improvements not in the initial implementation:

- Cluster details page with node metrics
- Logs viewer in UI
- Bulk operations (select multiple clusters to delete)
- Saved cluster templates
- Integration with Grafana dashboards
- Cluster health monitoring
- File upload/download via UI
- Multi-user authentication
- Cluster sharing/permissions

## Contributing

When modifying the UI:

1. Follow existing patterns from cluster-ui
2. Use TypeScript for type safety
3. Write SCSS modules for component styles
4. Test in both light and dark modes
5. Ensure responsive layout works
6. Update this README if adding new features

## License

Copyright 2024 The Cockroach Authors.
Licensed under the CockroachDB Software License.
