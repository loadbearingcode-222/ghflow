# 0001: Platform core — SeaweedFS, Postgres, Airflow 3 — pinned by tag and digest
Date: 2026-10-05 · Story: M0-S4 · Status: accepted

## Context

M0-S4 brings up the platform every later story runs on: object storage for raw, bronze, stream
and mart files; a database for Airflow's metadata and ghflow's operational state; and Airflow 3
to schedule the batch path. The plan named MinIO for storage. While pinning images on 2026-10-04,
MinIO's stopped resolving for anonymous pulls:

- `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z.hotfix.7aa24e772` and `quay.io/minio/mc:…` →
  HTTP 401 on the manifest, also by digest; a control (`quay.io/prometheus/prometheus:latest`)
  resolved, so quay.io itself was fine.
- `docker.io/minio/minio` → "pull access denied".

A stack that cannot be pulled from a clean machine is not reproducible, so MinIO was replaced
(owner's decision, verbatim 2026-10-05: "SeaweedFS 4.48 (Recommended)"). wikiflow and
olist-lakehouse still run MinIO from this machine's Docker cache; that is flagged, not fixed here
(gotcha #40).

## Options

1. **SeaweedFS 4.48** — Apache-2.0, since 2015, released 2026-09-28; one container runs master,
   volume, filer and S3 gateway. New to the owner.
2. **RustFS 1.0.1** — Apache-2.0, a MinIO drop-in; but 1.0 shipped 2026-10-03.
3. **Garage v2.4.1** — mature, but AGPL-3.0 and a cluster-layout bootstrap step.
4. **MinIO from the local cache** — no change, but unrebuildable and unverifiable.

## Decision

**Storage: SeaweedFS 4.48**, one `weed server -s3` container (`config/platform/seaweedfs-s3.sh`).
Every request must be signed: the identities file has no anonymous identity, and an unsigned GET
answers 403. Buckets `raw`, `bronze`, `stream`, `marts` are created by the `s3-init` one-shot,
which lists first because `s3.bucket.create` errors on an existing bucket.

A disposable probe on 2026-10-05 proved the client the plan depends on first: pyarrow 25.0.1 with
path-style addressing round-tripped Parquet, listed, wrote a 3,000,000-row object (multipart
size), deleted, and was refused with `ACCESS_DENIED` on a wrong secret. DuckDB's httpfs is
exercised in M1-S3, ClickHouse's `s3()` in M4.

**Database: Postgres 17.11**, one server, one database per tenant: `airflow` (metadata) and
`ghflow_ops` (owned by role `ghflow`), created by an idempotent, additive script that never drops
(`config/platform/postgres-init.sql`).

**Scheduler: Airflow 3.3.2**, LocalExecutor, the project in an isolated venv (olist's ADR-0007
pattern, `docker/airflow/Dockerfile`).

**Pins** — every third-party image by tag and digest, resolved anonymously and live:

| Image | Digest | Checked |
|---|---|---|
| `chrislusf/seaweedfs:4.48` | `sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d` | 2026-10-04 |
| `postgres:17.11-bookworm` | `sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652` | 2026-10-04 |
| `apache/airflow:3.3.2-python3.12` | `sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1` | 2026-10-04 |
| `ghcr.io/astral-sh/uv:0.11.25` | `sha256:1e3808aa9023d0980e7c15b1fa7c1ac16ff35925780cf5c459858b2d693f01a9` | 2026-10-04 |

CI's `compose render` job refuses a third-party image without a digest, and a Dockerfile base or
`COPY --from` without one (`IMAGE-UNPINNED`); images this repo builds (`ghflow/*`) are exempt
because their Dockerfile pins its bases.

**Memory:** the stack's limits total 5.4 GiB (5.8 GB) at M0, against a 14 GB budget (`automation/runs/m0-s4/platform.json`). `doctor` counts foreign stacks
at their `mem_limit`, not their live use: on 2026-10-05 olist-lakehouse used 6.3 GiB but was
allowed 18.7 GiB. Counting live use (olist's own doctor) would have passed with 20.7 GB "free";
counting limits refused with 8.7 GB, which is the true worst case.

**Backup obligations, in both directions:**

| Component | State class | Backup obligation | Why |
|---|---|---|---|
| SeaweedFS data (`raw`, `bronze`, `stream`, `marts`) | regenerable | **none** | GH Archive is the backup; backfill + replay re-derive every byte |
| Postgres `airflow` | regenerable | **none** | run history is rebuilt by backfill; nothing else lives there |
| Postgres `ghflow_ops` | regenerable | **none** | every number a story quotes is committed under `automation/runs/`; git is that evidence's backup |
| `.env` secrets | regenerable | **none**, kept local | losing it locks the volumes; recovery is `.\make.ps1 reset -Force` then re-derive |
| Committed evidence and code | irreplaceable | **yes: git + GitHub** | the record of what was measured and decided |

## Cost (honest)

- SeaweedFS is new: its operational quirks are unknown to us, and its image is 724 MB.
- The S3 API surface is broader than what one probe exercised; a client may still find a gap
  (the plan's ~15% risk). The fallback is RustFS 1.0.1, recorded as a superseding ADR.
- MinIO knowledge from wikiflow and olist does not transfer one-to-one (no `mc`; buckets via
  `weed shell`).

## Revisit when

- DuckDB's httpfs (M1-S3) or ClickHouse's `s3()` (M4) disagrees with SeaweedFS's S3 behaviour.
- MinIO images become publicly pullable again and a reason to unify with olist appears.
- A pinned digest stops resolving anonymously (re-run the pin check before any rebuild).
