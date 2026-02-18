# Graph Query Options for Neon

## Decision: Deferred to Step 4

No code changes needed now. This documents the research findings for future reference.

## Context

The benchmark uses plain PostgreSQL tables + recursive CTEs for graph queries on Neon serverless. The decision trail was: Neo4j too expensive (see [cost analysis](graph_database_cost_analysis.md)) -> no evaluation of PG graph extensions -> fell back to plain SQL tables.

## What Neon Actually Supports (neon.com/guides/graph-db)

### ltree (available now, just `CREATE EXTENSION`)

- Stores hierarchical/tree-structured data as label paths (e.g. `'A.B.C'`)
- Good for: org charts, category trees, file directories
- **Relevant for Step 4?** Possibly for referral chain hierarchy, but only for strict tree structures (no cycles, single parent). Won't help with billing rings or community detection.

### pgRouting (available now, just `CREATE EXTENSION`)

- Shortest-path and routing algorithms (Dijkstra, etc.) on network data
- Good for: finding optimal paths between nodes with costs
- **Relevant for Step 4?** Could help with shortest-path-based fraud queries (e.g., "how closely connected are these two providers?"). Won't help with cycle detection or community detection directly.

### Neither supports openCypher -- both are plain SQL.

## Apache AGE Findings

- **Mature**: v1.7.0, Apache 2.0 license, 4.2K GitHub stars, supports PG 11-18
- **Uses openCypher**: much cleaner than recursive CTEs for graph traversal
- **NOT available on Neon**: Neon's graph guide doesn't mention it. Would require Scale plan + custom extension request, or a hosting change.
- **Unproven at 376M entities**: public benchmarks max around ~2M
- **ETL/storage similar**: still needs graph table structure, no disk savings

## Why It Matters for Step 4 (Graph Analysis)

Recursive CTEs work for ingestion but will struggle with fraud detection graph queries:

| Query Type | Neon-Native Option | Notes |
|---|---|---|
| Referral chain detection (variable-depth traversal) | ltree | Only if chains are tree-shaped |
| Billing ring detection (cycle detection) | None | No Neon-native extension helps |
| Provider clustering (community detection) | None | No Neon-native extension helps |
| Shortest path between entities | pgRouting | Could handle this |

## When Step 4 Arrives

1. Enable pgRouting + ltree on Neon and evaluate for simpler graph queries first
2. Benchmark AGE locally against CTEs on complex graph queries (cycles, communities)
3. If AGE wins significantly, evaluate hosting alternatives (Azure PG, self-managed)
4. Also consider: PuppyGraph (no ETL), DuckDB duckpgq
