# TinyPG — Educational PostgreSQL Database Engine

A small Node.js implementation of a miniature relational database that demonstrates the core internals of PostgreSQL. Ships with an interactive shell *and* a browser-based explorer so you can watch every byte of state change as you type.

## What is TinyPG?

TinyPG is an educational project that builds a simplified but feature-complete database engine mirroring PostgreSQL's architecture. It demonstrates the fundamental concepts that make PostgreSQL (and most production databases) work correctly:

- **MVCC (Multi-Version Concurrency Control)** — Multiple transactions see different versions of the same row without locks
- **WAL (Write-Ahead Logging)** — Durability guarantee: changes are logged before written to disk
- **Crash Recovery** — Automatic replay of the write-ahead log to restore committed state
- **Buffer Management** — In-memory page cache with LRU eviction
- **Transaction Isolation** — READ COMMITTED semantics with snapshot-based visibility
- **Multi-Table Catalog** — Named tables with `CREATE TABLE` / `DROP TABLE`, persisted via WAL
- **Hash Indexes** — `CREATE INDEX name ON table(field)` with a runtime planner that picks index lookup vs. sequential scan and reports which it used

## What's new — Tables + Indexes (June 2026)

Earlier versions of TinyPG had a single implicit heap. The engine now has a real **catalog**:

- **`CREATE TABLE users`** allocates a named table. Each table owns its own list of heap pages — inserting into `users` never collides with another table's pages.
- **`CREATE INDEX idx_users_id ON users(id)`** builds an in-memory hash index from `field-value → list of (page id, page offset)`. A subsequent `SELECT * FROM users WHERE id = 5` skips the sequential scan and jumps straight to the row.
- **DDL is WAL-persisted.** `CREATE_TABLE`, `DROP_TABLE`, `CREATE_INDEX`, `DROP_INDEX` records are written and fsync'd. On restart, recovery replays them in order and (for indexes) rescans the heap to repopulate index entries — so after a crash your schema and indexes are restored along with your committed rows.
- **The planner is visible.** Every `SELECT WHERE` reports its plan — `index idx_users_id` or `seq scan (3 pages)` — in both the CLI (`-- plan: …`) and the GUI (a coloured badge under the result). You can watch the planner pick a different strategy the moment you add or drop an index.
- **Backwards compatible.** A default table called **`main`** is auto-created on every database boot, so the original `INSERT {...}` / `SELECT` / `DELETE WHERE …` flows (without `INTO/FROM`) still work — they target `main`.

This makes TinyPG concretely demonstrate the *query planning* concern, not just storage and concurrency. Toggle an index on and off and see the cost change in front of you.

## Architecture Overview

### Components

| Component | PostgreSQL Equivalent | Purpose |
|-----------|----------------------|---------|
| **WAL** | `pg_wal/` | Write-ahead log for durability; all mutations logged before applying |
| **BufferPool** | `shared_buffers` | Fixed-size in-memory cache of heap pages with LRU eviction |
| **Heap Pages** | Relation heap files | Stored tuples with MVCC metadata (xmin/xmax) |
| **Catalog** | `pg_class`, `pg_index` | In-memory registry of tables and indexes, persisted via WAL DDL records |
| **Index (hash)** | `pg_index` + B-tree files | `Map<value, [{pid, off}]>` — fast equality lookup, no range scan |
| **TxnMgr** | procarray + CLOG | Tracks running transactions and committed state |
| **Txn** | Transaction context | Provides `insert()`, `select()`, `delete()`, `lookup()`, `commit()` API |

### How It Works

1. **Insert**: Write-Ahead Log → find a page belonging to *this table* → Buffer Pool → mark dirty → update any indexes on this table → Commit flushes
2. **Select / Lookup**: Take snapshot of committed transactions → if an index covers the predicate, use it; else scan all pages owned by the table → filter by MVCC visibility
3. **Delete**: Mark tuples with xmax (logical delete) → Old version stays for MVCC readers (index still points at it; visibility filter discards it)
4. **DDL** (CREATE/DROP TABLE/INDEX): Write a single WAL record, fsync immediately (auto-commit) → update catalog in memory
5. **Crash Recovery**: Replay WAL — DDL records rebuild the catalog; INSERTs from committed transactions are re-applied; index backfill happens as inserts replay

## Getting Started

### Prerequisites

- **Node.js 14+** — install from [nodejs.org](https://nodejs.org/). No `npm install` needed; TinyPG has zero dependencies.
- Clone or download this folder. The first run creates `heap.db` and `wal.log` in the project directory.

### Three ways to use TinyPG

| Mode | Command | What you get |
|------|---------|--------------|
| **Demo** | `node tinypg.js` | Scripted 5-scene tour of MVCC + crash recovery |
| **CLI shell** | `node cli.js` | Interactive REPL (like `psql`) |
| **GUI explorer** | `node gui.js` then open <http://localhost:3000> | Browser UI (like a tiny pgAdmin) |

### Windows: double-click launchers

If you're on Windows you can skip the terminal entirely:

- **`TinyPG-GUI.bat`** — starts the GUI server *and* opens your default browser at `http://localhost:3000`
- **`TinyPG-CLI.bat`** — opens the interactive shell

Both launchers check for Node.js and tell you how to install it if it's missing. Close the console window (or press `Ctrl+C`) to stop the server cleanly — that flushes the buffer pool to disk.

### GUI walkthrough

`node gui.js` (or double-clicking `TinyPG-GUI.bat`) brings up a single page split into two columns:

**Left** — query input, result output, and a query-plan badge that shows whether the last `SELECT WHERE` used an index or a sequential scan.
**Right** — six live panels that refresh after every command:

- **Tables** — every table with its page list and live row count
- **Indexes** — every index with table, indexed field, and number of entries
- **Heap Pages** — every tuple on every page, labelled with the owning table, showing `xmin` / `xmax`. Deleted (`xmax != 0`) tuples are struck through but stay visible — that's MVCC keeping old versions.
- **WAL** — every write-ahead-log record (BEGIN / INSERT / DELETE / COMMIT / CREATE_TABLE / CREATE_INDEX / …) with its LSN, type, and txid.
- **Buffer Pool** — which pages are cached and which are dirty.
- **Transactions** — active txids, committed txids, next txid to be issued.

Open the page in **two browser windows** to demo MVCC: each tab gets its own session and its own transaction, so you can `BEGIN + INSERT` in one window and watch the other see nothing until you `COMMIT`.

### CLI walkthrough

```text
$ node cli.js
TinyPG shell — type .help for commands, .exit to quit.
tinypg> BEGIN
  BEGIN  (tx1)
tinypg> INSERT {"id": 1, "name": "alice"}
  INSERT 1  (tx1)
tinypg> COMMIT
  COMMIT (tx1)
tinypg> SELECT
  {"id":1,"name":"alice"}
  (1 row)
tinypg> SHOW WAL
  lsn=1  BEGIN    txid=1
  lsn=2  INSERT   txid=1  data={"id":1,"name":"alice"}
  lsn=3  COMMIT   txid=1
tinypg> .exit
bye.
```

Commands accepted in both CLI and GUI:

```sql
-- Transaction control
BEGIN | COMMIT | ROLLBACK

-- Schema (DDL — auto-commit, persisted to WAL)
CREATE TABLE <name>
DROP   TABLE <name>
CREATE INDEX <name> ON <table>(<field>)
DROP   INDEX <name>

-- Data (DML — runs inside the current transaction, auto-starts one if needed)
INSERT [INTO <table> [VALUES]] <json-object>
SELECT [* FROM <table>] [WHERE <field> = <value>]
DELETE [FROM <table>] WHERE <field> = <value>

-- Inspection — every SHOW target also has a live panel in the GUI
SHOW TABLES | SHOW INDEXES | SHOW WAL | SHOW PAGES | SHOW BUFFERS | SHOW TXNS
```

When you omit `INTO <table>` / `FROM <table>`, commands target the default `main` table (auto-created on first boot).

Every `SELECT` with a `WHERE` clause emits a plan line:
- `-- plan: index idx_users_id on users` — used a hash index, O(1) lookup
- `-- plan: seq scan (3 pages) on users` — no index covered this predicate, fell back to sequential scan

This is TinyPG's `EXPLAIN`. Drop the index and re-run the same SELECT to watch it switch.

### Worked example (paste into either CLI or GUI)

```sql
CREATE TABLE users
CREATE INDEX idx_uid ON users(id)
BEGIN
INSERT INTO users {"id": 1, "name": "alice"}
INSERT INTO users {"id": 2, "name": "bob"}
INSERT INTO users {"id": 3, "name": "carol"}
COMMIT
SELECT * FROM users WHERE id = 2          -- plan: index idx_uid
SELECT * FROM users WHERE name = "alice"  -- plan: seq scan (no index on name)
DELETE FROM users WHERE id = 1
SHOW PAGES                                -- alice's row is still there with xmax set
```

### The built-in demo

```bash
node tinypg.js
```

Runs a scripted 5-scene tour with no interaction:

1. **Scene 1**: Insert 5 rows in Transaction A (not yet committed)
2. **Scene 2**: Show MVCC — Transaction B sees nothing until A commits
3. **Scene 3**: Commit A; Transaction B takes fresh snapshot and sees the rows
4. **Scene 4**: Simulate crash — Transaction C inserts but process dies mid-transaction
5. **Scene 5**: Restart database — WAL recovery only restores committed rows

### Resetting state

The database persists across runs. To wipe it and start fresh, delete `heap.db` and `wal.log` from the project directory:

```bash
# bash
rm -f heap.db wal.log

# Windows cmd
del heap.db wal.log
```

Both files are gitignored, so they won't end up in commits.

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
