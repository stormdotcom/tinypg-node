# TinyPG — Educational PostgreSQL Database Engine

A ~300-line Node.js implementation of a miniature relational database that demonstrates the core internals of PostgreSQL. Learn how real databases manage transactions, consistency, and durability.

## What is TinyPG?

TinyPG is an educational project that builds a simplified but feature-complete database engine mirroring PostgreSQL's architecture. It demonstrates fundamental concepts that make PostgreSQL (and most production databases) work correctly:

- **MVCC (Multi-Version Concurrency Control)** — Multiple transactions see different versions of the same row without locks
- **WAL (Write-Ahead Logging)** — Durability guarantee: changes are logged before written to disk
- **Crash Recovery** — Automatic replay of the write-ahead log to restore committed state
- **Buffer Management** — In-memory page cache with LRU eviction
- **Transaction Isolation** — READ COMMITTED semantics with snapshot-based visibility

## Architecture Overview

### Components

| Component | PostgreSQL Equivalent | Purpose |
|-----------|----------------------|---------|
| **WAL** | `pg_wal/` | Write-ahead log for durability; all mutations logged before applying |
| **BufferPool** | `shared_buffers` | Fixed-size in-memory cache of heap pages with LRU eviction |
| **Heap Pages** | Relation heap files | Stored tuples with MVCC metadata (xmin/xmax) |
| **TxnMgr** | procarray + CLOG | Tracks running transactions and committed state |
| **Txn** | Transaction context | Provides `insert()`, `select()`, `delete()`, `commit()` API |

### How It Works

1. **Insert**: Write-Ahead Log → Buffer Pool → Mark dirty → Commit flushes
2. **Select**: Take snapshot of committed transactions → Scan pages → Filter by visibility
3. **Delete**: Mark tuples with xmax (logical delete) → Old version stays for MVCC readers
4. **Crash Recovery**: Replay WAL, only redo tuples from committed transactions

## Running the Demo

```bash
node tinypg.js
```

The script runs an integrated demo showing:

1. **Scene 1**: Insert 5 rows in Transaction A (not yet committed)
2. **Scene 2**: Show MVCC — Transaction B sees nothing until A commits
3. **Scene 3**: Commit A; Transaction B takes fresh snapshot and sees the rows
4. **Scene 4**: Simulate crash — Transaction C inserts but process dies mid-transaction
5. **Scene 5**: Restart database — WAL recovery only restores committed rows

### Expected Output

```
╔══════════════════════════════════════════╗
║       TinyPG — Educational Demo           ║
╚══════════════════════════════════════════╝

▶  Scene 1 — Insert 5 rows in txA
   txA (id=1) inserted 5 rows — NOT YET COMMITTED

▶  Scene 2 — MVCC: txB starts BEFORE txA commits
   txB (id=2) sees 0 rows — expected 0
   (txA's xmin not yet in committed set → invisible)

▶  Scene 3 — Commit txA; txB takes a fresh snapshot
   txA committed. txB now sees 5 rows — expected 5
   Rows: row-1, row-2, row-3, row-4, row-5

▶  Scene 4 — Crash mid-transaction
   ...

▶  Scene 5 — Restart & WAL replay (like pg startup process)
   After recovery: 5 rows visible — expected 5
   ...

✓  TinyPG behaves like a real ACID database
```

## Key Learning Points

### 1. MVCC Visibility

A tuple is visible to a transaction's snapshot when:
- Its inserter (xmin) had committed **before** the snapshot was taken, AND
- Its deleter (xmax) had **not** committed before the snapshot, OR xmax is 0 (not deleted)

This allows multiple readers to see different versions of the same logical row without blocking.

### 2. WAL-Before-Page Rule

Every mutation follows: `log to WAL → fsync() → modify in-memory page`

If a crash occurs between the WAL write and the page reaching disk, recovery can redo the operation from the log. This is the foundational principle of durability.

### 3. Buffer Pool Eviction

The buffer pool is fixed-size (8 pages in this demo). When full:
- Prefer evicting clean (unmodified) pages (no disk I/O)
- If all are dirty, flush the LRU dirty page to disk first

This is why `MarkBufferDirty()` is critical — without marking a page dirty, an LRU page could be evicted and lose your changes.

### 4. Transaction Snapshots

`READ COMMITTED` isolation is implemented by taking a fresh snapshot at the start of each statement:
```javascript
const snap = this.mgr.snapshot(this.id);
// snap.committed = set of txids that had committed as of this moment
// snap.myTxid = our transaction id
```

A tuple from txn X is visible if X is in our snapshot's committed set (or X is our own txid).

### 5. Crash Recovery

On startup, the database:
1. Reads the entire WAL file
2. **Pass 1**: Identifies which transactions committed (BUILD COMMITTED SET)
3. **Pass 2**: Truncates heap to catalog page only, then replays all committed INSERTs

Uncommitted changes never reappear — their xmin is never added to the committed set, so MVCC visibility filters them out.

## File Format

### `heap.db` (Data File)

Pages are 4096 bytes each:

```
[Page 0: Catalog]
  Pages 1+: Heap pages

Each page:
  Offset 0–3:   Magic (0xDEADBEEF)
  Offset 4–7:   Number of tuples
  Offset 8–11:  Free offset (next write position)
  Offset 12+:   Tuples
```

### `wal.log` (Write-Ahead Log)

Newline-delimited JSON records (human-readable):

```json
{"type":"BEGIN","txid":1,"lsn":1}
{"type":"INSERT","txid":1,"data":"{\"id\":1,\"name\":\"row-1\",\"value\":10}","lsn":2}
{"type":"COMMIT","txid":1,"lsn":3}
```

## Understanding the Code

### Classes

- **`WAL`** — Append-only log; assign LSN and fsync on commits
- **`BufferPool`** — LRU page cache; fetch(), dirty(), checkpoint()
- **`TxnMgr`** — Allocate txids; track committed txns; create snapshots
- **`Txn`** — User-facing API: insert(), select(), delete(), commit()
- **`Database`** — Orchestrate components; run WAL recovery on startup

### Key Methods

| Method | Purpose |
|--------|---------|
| `db.begin()` | Start a new transaction |
| `txn.insert(row)` | Add a row; logs to WAL before modifying buffer |
| `txn.select(pred)` | Scan pages; filter by MVCC visibility rules |
| `txn.delete(pred)` | Mark matching rows with xmax; logical delete |
| `txn.commit()` | Write COMMIT to WAL, fsync, update committed set |
| `db.close()` | Checkpoint dirty pages, close file handles |

## Configuration

Edit these constants in the source to experiment:

```javascript
const PAGE_SIZE     = 4096;     // Heap page size (PostgreSQL default is 8192)
const MAX_BUF_PAGES = 8;        // Buffer pool size (shared_buffers equivalent)
const DATA_FILE     = 'heap.db'; // Data file name
const WAL_FILE      = 'wal.log'; // WAL file name
```

## Educational Value

This project is designed for:

- **Students** learning database internals
- **Developers** curious about how PostgreSQL works
- **Engineers** studying MVCC and transaction isolation
- **Anyone** wanting to understand durability guarantees in production systems

It omits production features (query parsing, optimization, secondary indexes, locking) but includes the core techniques that make databases work correctly and reliably.

## Limitations & Trade-offs

| Aspect | TinyPG | PostgreSQL |
|--------|--------|------------|
| Query Language | JSON objects only | Full SQL |
| Indexing | None (sequential scan) | B-tree, hash, GiST, etc. |
| Concurrency Control | Snapshot isolation via MVCC | Fine-grained locking + MVCC |
| WAL Format | JSON (readable) | Binary (compact, optimized) |
| Checkpointing | Manual `checkpoint()` | Continuous background process |
| Recovery | Simple full replay | Intelligent redo from checkpoint LSN |
| Page Size | 4KB | Configurable (default 8KB) |

## See Also

- PostgreSQL source: [src/backend/storage/](https://github.com/postgres/postgres/tree/master/src/backend/storage)
- Transaction visibility: [heapam_visibility.c](https://github.com/postgres/postgres/blob/master/src/backend/access/heap/heapam_visibility.c)
- WAL format: [xlog.h](https://github.com/postgres/postgres/blob/master/src/include/access/xlog.h)
- MVCC design: [PostgreSQL Documentation](https://www.postgresql.org/docs/current/mvcc.html)

---

**TinyPG** © 2025 — Educational demonstration of PostgreSQL internals.
