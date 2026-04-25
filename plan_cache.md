# FBD Cache / Offline Plan

## Objective

Restore offline reuse in `FBD` without changing the public API, so a dataset that was already resolved and downloaded can still be parsed when the edge function or the network is unavailable.

## Strategy

The cache mode should be **online-first with offline fallback**, not `cached-first`.

This means:

1. try to resolve metadata from the edge function first
2. if that succeeds, update local metadata cache
3. if remote lookup fails, fall back to cached metadata
4. only use offline mode when both:
   - cached metadata exists
   - the downloaded local file exists

This keeps `FBD` aligned with possible remote updates in:

- `filename`
- `header`
- `parser_type`
- `parse_config`

## Metadata Format

Metadata is stored as **one JSON file per dataset**.

Location:

- `Config.CACHE_DIR / "metadata" / "<dataset>.metadata.json"`

Example:

```json
{
  "status": "ok",
  "dataset": "gene_association",
  "filename": "gene_association.fb.gz",
  "header": null,
  "parser_type": "fb",
  "parse_config": {
    "start_line": 5,
    "columns": [
      "DB",
      "DB Object ID",
      "DB Object Symbol"
    ]
  }
}
```

The local cache should not persist `link`, so users do not get direct dataset URLs from cached metadata files.

This layout was chosen because it:

- simplifies invalidation
- is easy to inspect manually
- avoids rewriting a single global cache file
- limits corruption to one dataset at a time

## Current Implementation Status

Implemented:

- metadata persistence per dataset in JSON
- offline fallback when edge metadata lookup fails
- reuse of the already downloaded local file
- safe behavior when `CACHE_DIR` is not writable

Not implemented yet:

- metadata expiration / invalidation policy
- explicit cache cleanup utilities
- user-facing forced refresh controls

## Behavior Covered

### Case 1. Online normal flow

- resolve metadata from Supabase
- download or reuse the local file
- parse normally
- update cached metadata

### Case 2. Offline with complete cache

- edge lookup fails
- cached metadata exists
- local file exists
- parsing still succeeds

### Case 3. Offline without metadata cache

- edge lookup fails
- no cached metadata exists
- return a clear error

### Case 4. Offline with metadata but without local file

- cached metadata exists
- local file does not exist
- return a clear error

## Design Constraints

- Keep the public API unchanged.
- Do not reintroduce dataset-specific hardcodes in `Downloader`.
- Preserve the parser-driven design based on `parser_type` and `parse_config`.
- Keep download and parse layers separated.

## Validation

Tests already added cover:

- metadata cache file creation on successful download
- offline recovery with cached metadata + local file
- offline failure when metadata cache is missing

Still worth adding later:

- corrupted metadata cache file
- metadata cache present but local file missing
- end-to-end manual check for more than one dataset type

## Next Step

Manually validate `gene_association` end-to-end:

1. successful online download
2. metadata cache creation
3. simulated offline execution
4. successful parse from local file + cached metadata

After that, the same mechanism can be reused for the rest of the datasets without changing the public API.
