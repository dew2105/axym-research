# Graph Database Cost Analysis

## Evaluated Options

### Neo4j AuraDB
- **Free tier**: 200K nodes / 400K relationships (our dataset: 1.81M nodes, 227M+ rels — 9x over node limit)
- **Professional**: $65/GB/month. 64 GB instance needed → $4,160/month ($5.76/hr)
- **Verdict**: Free tier requires 99.8% sampling. Professional too expensive for research benchmark.

### TigerGraph Savanna
- **Free tier**: Credit-based trial only (no persistent free tier)
- **Paid**: TG-1 (64 GB) = $4/hr ($2,920/month). ~30% cheaper than Neo4j per GB.
- **Verdict**: No persistent free option. Paid tier still expensive for research.

### PostgreSQL Graph Tables (Selected)
- **Cost**: $0 incremental (reuses existing Neon instance)
- **Approach**: Explicit node/edge tables + recursive CTEs for traversal
- **Tradeoff**: No native graph indexing or Cypher. Graph queries are verbose SQL.
- **Why this works for benchmarking**: Demonstrates the ETL overhead of transforming tabular → graph structure, which is the key finding about impedance mismatch.

## Full Dataset Sizing

| Metric | Count |
|--------|-------|
| Total rows | 227,083,361 |
| Unique providers (nodes) | 1,802,136 |
| Unique procedures (nodes) | 10,881 |
| Total nodes | 1,813,017 |
| BILLED_FOR relationships | ~227M |

## Cost Comparison (full dataset, 4-hour benchmark)

| Platform | Instance | $/hour | 4-hr cost | Monthly |
|----------|----------|--------|-----------|---------|
| Neo4j AuraDB Pro | 64 GB | $5.76 | $23.04 | $4,160 |
| TigerGraph Savanna | TG-1 (64 GB) | $4.00 | $16.00 | $2,920 |
| PostgreSQL (Neon) | Already provisioned | $0 | $0 | $0 incremental |
