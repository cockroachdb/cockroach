# Naming Policy (conventional, graceful)
- Put the file **in the closest package directory** to the code it explains.
- Prefer a **single, conventional name**:
  1. `OVERVIEW.md` (preferred if not present)
  2. If `OVERVIEW.md` exists, use `ARCHITECTURE.md`
  3. If both exist, use `<TOPIC>_OVERVIEW.md` (e.g., `REPLICATION_OVERVIEW.md`)
- The document must be Markdown and self-contained (no images required; ASCII diagrams OK).
- Immediately add/update a bullet in the repo root `CLAUDE.md` under “Domain Context Files” with the final relative path and a one-line description.
