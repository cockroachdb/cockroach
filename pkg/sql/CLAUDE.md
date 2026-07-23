# SQL Package Development Guide

## SQL Column Naming Standards

**Principles for SHOW statements and virtual tables:**
- **Consistent casing** across all statements
- **Same concept = same word**: `variable`/`value` between different SHOW commands
- **Usable without quotes**: `start_key` not `"Start Key"`, avoid SQL keywords
- **Underscore separation**: `start_key` not `startkey` (consistent with information_schema)
- **Avoid abbreviations**: `id` OK (common), `loc` not OK (use `location`)
- **Disambiguate primary key columns**: `zone_id`, `job_id`, `table_id` not just `id`
- **Specify handle type**: `table_name` vs `table_id` to allow both in future
- **Match information_schema**: Use same labels when possible and appropriate
