# DB Console

## Architecture

Read `pkg/ui/ARCHITECTURE.md` before making architectural decisions
about data fetching, component patterns, or workspace structure.

Key conventions:
- Use SWR hooks (`useSwrWithClusterId`) for all new data fetching — not Redux
- New components must be functional components with hooks
