# DeFi Risk Lakehouse

A production-grade lakehouse architecture that tracks open borrow
positions across **Aave V3**, **Compound V3**, and **MakerDAO** via The Graph
subgraph APIs, computes health factors and collateral-at-risk, and models
liquidation cascade scenarios (e.g. impact of a 20% ETH price drop on
protocol solvency).

Everything runs locally via **Docker Compose** — 100% free, no cloud accounts
required.

---

## Architecture

```
The Graph APIs                 MinIO (S3)                  Nessie
(Aave / Compound / Maker)   s3://lakehouse/raw/          (Iceberg catalog)
         │                         │                           │
         │   Airflow DAG           │   Spark Bronze job        │
         └──► defi_ingest ────────►│──────────────────────────►│ nessie.bronze.*
                                   │                           │
                                   │   Spark Silver job        │
                                   │──────────────────────────►│ nessie.silver.*
                                   │                           │
                              Trino (SQL)                      │
                                   │   dbt-trino               │
                                   └──────────────────────────►│ nessie.gold.*
                                                               │
                                                        ┌──────┴───────┐
                                                        │  fct_health_ │
                                                        │   factors    │
                                                        │  fct_cascade_│
                                                        │  scenarios   │
                                                        └──────────────┘
```

### Services

| Service | Port | Purpose |
|---|---|---|
| Airflow Webserver | 8080 | DAG monitoring UI |
| Trino | 8081 | SQL query engine for dbt |
| Spark Master | 8082 | Spark cluster UI |
| MinIO Console | 9001 | Object storage browser |
| Nessie | 19120 | Iceberg REST catalog |
| PostgreSQL | 5432 | Airflow metadata DB |

### Data Layers

| Layer | Namespace | Written by | Content |
|---|---|---|---|
| Raw | `s3://lakehouse/raw/` | Python (Airflow) | NDJSON from The Graph |
| Bronze | `nessie.bronze.*` | Spark | Raw JSON → Iceberg |
| Silver | `nessie.silver.*` | Spark | Normalised, USD-valued |
| Gold | `nessie.gold.*` | dbt (Trino) | Health factors, risk metrics |

---

## Quick Start

### Prerequisites

- Docker Desktop (4+ GB RAM allocated to Docker recommended)
- `make`
- Python 3.11+ (for local development/testing only)

### 1. Clone and initialise

```bash
git clone <repo-url>
cd defi-risk-lakehouse

# First-time setup: copies .env, builds images, starts services
make init
```

### 2. (Optional) Add a The Graph API key

The ingestion clients work without an API key using the hosted service,
but a free key from [thegraph.com/studio](https://thegraph.com/studio)
gives you 100K queries/month on the decentralised network.

```bash
# Edit .env and set:
GRAPH_API_KEY=your-key-here
```

### 3. Trigger the pipeline

```bash
# Trigger the ingestion DAG (fetches data from The Graph)
make trigger-ingest

# Or trigger just the transform DAG (if raw data is already in MinIO)
make trigger-transform
```

Or navigate to http://localhost:8080 (admin / admin) and trigger `defi_ingest` manually.

---

## Development

```bash
# Install Python dev dependencies locally
pip install -r requirements-dev.txt

# Run unit tests (no Docker required)
make test-unit

# Lint + format
make lint
make format

# Run dbt models manually
make dbt-run

# Run dbt tests
make dbt-test

# Open a Trino SQL shell
make trino-shell
```

---

## dbt Models

```
models/
├── staging/
│   ├── stg_aave__positions.sql       — Aave V3 userReserves
│   ├── stg_compound__positions.sql   — Compound V3 borrow positions
│   └── stg_maker__vaults.sql         — MakerDAO CDPs
├── intermediate/
│   ├── int_positions_unified.sql     — UNION ALL across protocols
│   └── int_collateral_weighted.sql   — Per-user weighted liquidation threshold
└── marts/
    ├── fct_health_factors.sql        — HF per user × protocol, risk tiers
    ├── fct_liquidation_risk.sql      — Protocol risk summary by tier
    └── fct_cascade_scenarios.sql     — Liquidation cascade for 5 ETH shock scenarios
```

### Health Factor Formula

```
HF = Σ(collateral_i × liq_threshold_i × price_i) / Σ(debt_j × price_j)

HF < 1.0  → LIQUIDATABLE (can be liquidated now)
HF < 1.05 → CRITICAL
HF < 1.10 → AT_RISK
HF < 1.25 → WATCH
HF ≥ 1.25 → HEALTHY
```

### Cascade Scenarios

The `fct_cascade_scenarios` model cross-joins all positions with price shock
scenarios (defined in `seeds/price_shock_scenarios.csv`) and computes:

- How many positions become liquidatable
- Total debt at risk (USD)
- Protocol insolvency exposure (debt not covered by seized collateral)

| Scenario | ETH Drop |
|---|---|
| base | 0% |
| mild_eth_drop | 10% |
| moderate_eth_drop | 20% |
| severe_eth_drop | 30% |
| black_swan_eth | 40% |
| market_crash | 50% |

---

## Testing

```
tests/
├── unit/                          # No external services required
│   ├── test_health_factor.py      # Formula correctness, edge cases
│   ├── test_graph_client.py       # HTTP client pagination + retry
│   ├── test_aave_client.py        # Aave URL selection, response parsing
│   └── test_maker_client.py       # WAD/RAD unit conversion math
└── integration/                   # Requires Docker Compose to be running
    ├── conftest.py                # MinIO fixtures
    └── test_dag_integrity.py      # Airflow DAG import + structure
```

```bash
make test-unit           # Unit tests only
make test-integration    # Full integration suite (starts Docker first)
```

---

## CI/CD

GitHub Actions pipeline (`.github/workflows/ci.yml`):

| Job | Trigger | What it does |
|---|---|---|
| `lint` | Every push | `ruff check` + `ruff format --check` |
| `unit-tests` | Every push | `pytest tests/unit/` |
| `dag-tests` | Every push | Airflow DAG import + structure validation |
| `dbt-parse` | Every push | `dbt parse` — validates SQL without DB connection |
| `docker-build` | Every push | Builds Airflow image (no push) |
| `integration-tests` | Push to `main` | Full Docker Compose stack + integration tests |

---

## Key Technology Choices

| Concern | Choice | Why |
|---|---|---|
| Iceberg catalog | Nessie | Git-like branching for data; both Spark and Trino share the same catalog via REST |
| Object storage | MinIO | Free S3-compatible, runs in Docker |
| SQL engine | Trino | dbt-trino is mature; Trino reads Iceberg via the same Nessie REST catalog Spark writes to |
| Spark ↔ Trino data sharing | Nessie REST catalog | Both engines point at the same Nessie server; data written by Spark is immediately queryable by Trino with no additional registration step |
| Airflow executor | LocalExecutor | Zero-overhead for local dev; production-equivalent by just changing `AIRFLOW__CORE__EXECUTOR` |
| Spark JARs | `iceberg-aws-bundle` | Avoids classpath conflicts; the AWS SDK v2-based bundle is the Iceberg project's official recommendation for Iceberg 1.5+ |
