# kleio-core

Shared portable domain types and `Store` interface for the [Kleio](https://kleio.build) ecosystem.

## Purpose

`kleio-core` provides the common type definitions that both **kleio-cli** (local OSS tool) and **kleio-app** (cloud SaaS) import. This prevents domain type divergence between the two codebases while keeping each free to implement storage and business logic independently.

## Contents

- **Domain types:** `Event`, `BacklogItem`, `Commit`, `FileChange`, `Link`, `Identifier`, `Repo`
- **Store interface:** The contract that local (SQLite) and cloud (HTTP API) backends implement
- **Enums:** Signal types, source types, link types, statuses, categories, urgency/importance levels
- **Work item vocabulary:** Canonical lifecycle statuses (`WorkItemStatus*`) and granularity constants (`WorkItemGranularity*`), plus `WorkItemVocabulary` normalization from optional workspace alias maps — see [API_CONTRACT_WORK_ITEMS.md](./API_CONTRACT_WORK_ITEMS.md) §Canonical vocabulary.

## Usage

```go
import "github.com/kleio-build/kleio-core"
```

## Consumers

| Repo | Role |
|------|------|
| `kleio-cli` | Implements `kleio.Store` with `localdb.Store` (SQLite) and `apistore.Store` (HTTP) |
| `kleio-app` | Uses domain types for API compatibility and sync contract |

## License

MIT — see [LICENSE](LICENSE).
