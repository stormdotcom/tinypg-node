'use strict';
/**
 * tinypg.js — ~300-line educational database demonstrating PostgreSQL internals
 *
 * PostgreSQL component map:
 *   WAL class         → pg_wal/  (write-ahead log directory)
 *   BufferPool class  → shared_buffers  (in-memory page cache)
 *   heap pages        → relation heap files  (base/<db_oid>/<rel_oid>)
 *   xmin / xmax       → HeapTupleHeader.t_xmin / t_xmax
 *   isVisible()       → HeapTupleSatisfiesSnapshot()
 *   TxnMgr class      → procarray + pg_xact (CLOG)
 *   _recover()        → startup process "redo" phase after crash
 */
const fs   = require('fs');
const path = require('path');

// ── Constants ────────────────────────────────────────────────────────────────
// In PostgreSQL these are compile-time constants or GUC settings.
const PAGE_SIZE     = 4096;  // pg default is 8192 (block_size); 4k keeps output readable
const MAX_BUF_PAGES = 8;     // like shared_buffers = 8 pages — tiny pool forces eviction
const DATA_FILE     = 'heap.db';
const WAL_FILE      = 'wal.log';
const PAGE_HDR_SZ   = 12;   // magic(4) + num_tuples(4) + free_off(4)
const TUPLE_HDR_SZ  = 16;   // tuple_len(4) + xmin(4) + xmax(4) + data_len(4)
const PAGE_MAGIC    = 0xDEADBEEF; // sanity sentinel — pg uses pd_checksum

// ── Page Helpers ─────────────────────────────────────────────────────────────
// A page is a fixed-size Buffer.  Layout mirrors pg's PageHeaderData + item area.
// pg uses pd_lower/pd_upper (growing inward from both ends of the page);
// we grow forward from one end only — simpler, loses no educational clarity.

function newPage() {
  const b = Buffer.alloc(PAGE_SIZE, 0);
  b.writeUInt32BE(PAGE_MAGIC,  0);   // identify valid pages (like pg's pd_lsn / pd_checksum)
  b.writeUInt32BE(0,           4);   // num_tuples
  b.writeUInt32BE(PAGE_HDR_SZ, 8);  // free_offset — next write position
  return b;
}

// Tiny accessors — pg reads these from PageHeaderData struct fields
const numTuples  = p => p.readUInt32BE(4);
const freeOffset = p => p.readUInt32BE(8);
const setFree    = (p, v) => p.writeUInt32BE(v, 8);
const setNumTup  = (p, v) => p.writeUInt32BE(v, 4);
const freeSpace  = p => PAGE_SIZE - freeOffset(p);

// Write one tuple onto the page — like pg's PageAddItemExtended() inside heap_insert().
// Returns the byte offset the tuple was written at, or -1 if the page is full.
function appendTuple(page, xmin, xmax, dataStr) {
  const data = Buffer.from(dataStr, 'utf8');
  const len  = TUPLE_HDR_SZ + data.length;
  const off  = freeOffset(page);
  if (off + len > PAGE_SIZE) return -1; // page full — caller must find another page

  page.writeUInt32BE(len,         off);
  page.writeUInt32BE(xmin,        off + 4);
  page.writeUInt32BE(xmax,        off + 8);  // 0 = tuple is alive (not deleted)
  page.writeUInt32BE(data.length, off + 12);
  data.copy(page, off + 16);

  setFree(page, off + len);
  setNumTup(page, numTuples(page) + 1);
  return off;
}

// Walk every tuple on a page — like pg's heap_getnext() / PageGetItemId() loop.
// cb receives (xmin, xmax, dataStr, byteOffset).
function scanPage(page, cb) {
  let off = PAGE_HDR_SZ;
  const n = numTuples(page);
  for (let i = 0; i < n; i++) {
    const len     = page.readUInt32BE(off);
    const xmin    = page.readUInt32BE(off + 4);
    const xmax    = page.readUInt32BE(off + 8);
    const dataLen = page.readUInt32BE(off + 12);
    const data    = page.slice(off + 16, off + 16 + dataLen).toString('utf8');
    cb(xmin, xmax, data, off);
    off += len;
  }
}

// Stamp xmax on an existing tuple — pg's heap_delete() sets HeapTupleHeader.t_xmax.
// This is a logical delete: the old tuple version stays on disk for MVCC readers.
function setXmax(page, tupleOff, xmax) {
  page.writeUInt32BE(xmax, tupleOff + 8);
}

// ── WAL ──────────────────────────────────────────────────────────────────────
// Like pg_wal/: every mutation must be durably logged BEFORE the page is changed.
// That "WAL before page" rule is what lets us recover after a crash.
// pg uses compact binary XLogRecord structs; we use newline-delimited JSON
// so you can open wal.log and read every decision the engine made.

class WAL {
  constructor(file) {
    this.file = file;
    this.lsn  = 0; // Log Sequence Number — monotonically increasing byte offset in pg

    if (!fs.existsSync(file)) fs.writeFileSync(file, '');
    // Keep an fd open purely for fsync() — appendFileSync opens/closes on its own
    this.fd = fs.openSync(file, 'r+');

    // Restore LSN so we don't reuse sequence numbers after a restart
    const lines = fs.readFileSync(file, 'utf8').split('\n').filter(Boolean);
    if (lines.length) this.lsn = JSON.parse(lines[lines.length - 1]).lsn;
  }

  // XLogInsert() — assign an LSN and append the record to the log file
  write(rec) {
    rec.lsn = ++this.lsn;
    fs.appendFileSync(this.file, JSON.stringify(rec) + '\n');
    return rec.lsn;
  }

  // XLogFlush() — called at COMMIT to guarantee durability before ACKing the client.
  // Without this fsync, a crash between write() and disk flush could lose a commit.
  flush() { fs.fsyncSync(this.fd); }

  readAll() {
    return fs.readFileSync(this.file, 'utf8').split('\n').filter(Boolean).map(JSON.parse);
  }

  close() { fs.closeSync(this.fd); }
}

// ── Buffer Pool ───────────────────────────────────────────────────────────────
// Mirrors pg's shared_buffers: a fixed pool of in-memory page frames shared by
// all backends.  pg uses a clock-sweep replacement algorithm; we use simple LRU.
// The key invariant: a dirty page (modified in memory) must be written to disk
// before its WAL records can be discarded — the "no steal before WAL flush" rule.

class BufferPool {
  constructor(file, maxPages) {
    this.maxPages = maxPages;
    this.frames   = new Map();   // page_id → { buf: Buffer, dirty: bool }
    this.lru      = [];          // page_ids oldest→newest (evict from front)

    if (!fs.existsSync(file)) fs.writeFileSync(file, Buffer.alloc(PAGE_SIZE));
    this.fd = fs.openSync(file, 'r+');

    // Initialise catalog page (page 0) if this is a brand-new file
    const probe = Buffer.alloc(PAGE_SIZE);
    fs.readSync(this.fd, probe, 0, PAGE_SIZE, 0);
    if (probe.readUInt32BE(0) !== PAGE_MAGIC) {
      fs.writeSync(this.fd, newPage(), 0, PAGE_SIZE, 0); // write catalog page
    }
  }

  // ReadBuffer() — return the Buffer for a page, loading from disk on a cache miss
  fetch(pid) {
    if (this.frames.has(pid)) {
      // Cache hit — promote to MRU position (like pg's buffer pin + clock-sweep advance)
      this.lru = this.lru.filter(id => id !== pid);
      this.lru.push(pid);
      return this.frames.get(pid).buf;
    }
    // Cache miss — evict if the pool is full, then load
    if (this.frames.size >= this.maxPages) this._evict();

    const buf      = Buffer.alloc(PAGE_SIZE);
    const fileSize = fs.fstatSync(this.fd).size;
    if (pid * PAGE_SIZE < fileSize) {
      fs.readSync(this.fd, buf, 0, PAGE_SIZE, pid * PAGE_SIZE); // smgrread()
    } else {
      newPage().copy(buf); // page beyond EOF — like pg extending a relation (smgrextend)
    }
    this.frames.set(pid, { buf, dirty: false });
    this.lru.push(pid);
    return buf;
  }

  // MarkBufferDirty() — must be called after every in-place page mutation
  dirty(pid) {
    const e = this.frames.get(pid);
    if (e) e.dirty = true;
  }

  // StrategyGetBuffer() — find a victim frame using LRU order
  _evict() {
    // Prefer evicting a clean page (no disk I/O needed)
    for (let i = 0; i < this.lru.length; i++) {
      if (!this.frames.get(this.lru[i]).dirty) {
        this.frames.delete(this.lru.splice(i, 1)[0]);
        return;
      }
    }
    // All frames dirty — flush the LRU dirty page (in pg this triggers bgwriter)
    const pid = this.lru.shift();
    this._writePage(pid);
    this.frames.delete(pid);
  }

  _writePage(pid) {
    const e = this.frames.get(pid);
    if (e && e.dirty) {
      fs.writeSync(this.fd, e.buf, 0, PAGE_SIZE, pid * PAGE_SIZE); // smgrwrite()
      e.dirty = false;
    }
  }

  // BufferSync() — checkpoint: flush all dirty frames, then fsync the data file.
  // pg's checkpointer does this periodically so WAL replay can start from a
  // recent point rather than replaying from the beginning of time.
  checkpoint() {
    for (const pid of this.frames.keys()) this._writePage(pid);
    fs.fsyncSync(this.fd);
  }

  // Truncate the heap file back to just the catalog page and clear all frames.
  // Called at the start of WAL recovery so we replay onto a known-clean slate.
  reset() {
    this.frames.clear();
    this.lru = [];
    fs.ftruncateSync(this.fd, PAGE_SIZE); // keep catalog page only
  }

  close() { this.checkpoint(); fs.closeSync(this.fd); }
}

// ── MVCC Visibility ───────────────────────────────────────────────────────────
// HeapTupleSatisfiesSnapshot() in pg/src/backend/access/heap/heapam_visibility.c
//
// A tuple version is visible to a snapshot when:
//   (a) its inserter (xmin) had committed BEFORE the snapshot was taken, AND
//   (b) its deleter (xmax) had NOT committed before the snapshot  (or xmax == 0)
//
// This lets multiple transactions see different versions of the same logical row
// without locking — the core idea behind MVCC.

function isVisible(xmin, xmax, snap) {
  // xmin must be committed in our snapshot, OR it is our own txid (we see our own inserts)
  const xminOk = snap.committed.has(xmin) || xmin === snap.myTxid;
  if (!xminOk) return false; // inserter hasn't committed — invisible

  if (xmax !== 0) {
    // Someone logically deleted this tuple; if they committed before our snapshot, it's gone
    if (snap.committed.has(xmax)) return false;
  }
  return true;
}

// ── Transaction Manager ───────────────────────────────────────────────────────
// Combines pg's procarray (tracks running backends/txns),
// pg_xact / CLOG (committed / aborted status per txid),
// and GetSnapshotData() (builds the visibility snapshot for a read).

class TxnMgr {
  constructor() {
    this.nextId    = 1;         // next TransactionId — pg wraps at 2^32, we don't bother
    this.committed = new Set(); // like CLOG pages marked TRANSACTION_STATUS_COMMITTED
    this.active    = new Map(); // txid → Txn  (like pg's PGPROC entries in procarray)
  }

  begin(db) {
    const txn = new Txn(this.nextId++, this, db);
    this.active.set(txn.id, txn);
    return txn;
  }

  commit(id)   { this.committed.add(id); this.active.delete(id); }

  rollback(id) {
    // pg writes TRANSACTION_STATUS_ABORTED to the CLOG page for this txid.
    // We just drop it from active — tuples with this xmin stay on disk but are
    // invisible because their xmin is never in the committed set.
    this.active.delete(id);
  }

  // GetSnapshotData() — capture an immutable view of the committed set right now.
  // Any txid NOT in committed (and not our own) is considered "in-progress" for
  // visibility purposes — we won't see those tuples.
  snapshot(myTxid) {
    return {
      myTxid,
      committed:  new Set(this.committed), // copy = "as of this instant"
    };
  }
}

// ── Transaction ───────────────────────────────────────────────────────────────

class Txn {
  constructor(id, mgr, db) {
    this.id   = id;
    this.mgr  = mgr;
    this.db   = db;
    this.done = false;
  }

  // heap_insert() — WAL record first, then mutate the buffer pool page.
  // The WAL-before-page rule: if we crash after the WAL write but before the
  // page hits disk, recovery can redo the insert from the WAL record.
  insert(row) {
    if (this.done) throw new Error('transaction has ended');
    const data = JSON.stringify(row);
    this.db.wal.write({ type: 'INSERT', txid: this.id, data }); // WAL FIRST
    const pid  = this.db._findPage(data);
    const page = this.db.buf.fetch(pid);
    appendTuple(page, this.id, 0, data); // xmax=0 means alive
    this.db.buf.dirty(pid);
  }

  // SeqScan executor node — full sequential scan of every heap page.
  // Applies MVCC filter using a fresh snapshot (READ COMMITTED semantics:
  // each statement sees all commits that happened before it started).
  select(pred) {
    if (this.done) throw new Error('transaction has ended');
    const snap = this.mgr.snapshot(this.id); // GetSnapshotData()
    const rows = [];
    for (let pid = 1; pid < this.db.nextPid; pid++) { // skip page 0 (catalog)
      scanPage(this.db.buf.fetch(pid), (xmin, xmax, data) => {
        if (isVisible(xmin, xmax, snap)) {
          const row = JSON.parse(data);
          if (!pred || pred(row)) rows.push(row);
        }
      });
    }
    return rows;
  }

  // heap_delete() — mark matching tuples with xmax = our txid.
  // The old version stays on disk so concurrent MVCC readers can still see it.
  delete(pred) {
    if (this.done) throw new Error('transaction has ended');
    const snap = this.mgr.snapshot(this.id);
    for (let pid = 1; pid < this.db.nextPid; pid++) {
      const page = this.db.buf.fetch(pid);
      scanPage(page, (xmin, xmax, data, off) => {
        if (isVisible(xmin, xmax, snap) && pred(JSON.parse(data))) {
          this.db.wal.write({ type: 'DELETE', txid: this.id, pid, off }); // WAL FIRST
          setXmax(page, off, this.id);
          this.db.buf.dirty(pid);
        }
      });
    }
  }

  // CommitTransaction() — write COMMIT to WAL, fsync, then update in-memory state.
  // The fsync on the COMMIT record is the moment of durability: if we crash after
  // this returns, recovery will find the COMMIT record and redo our work.
  commit() {
    if (this.done) throw new Error('transaction has ended');
    this.done = true;
    this.db.wal.write({ type: 'COMMIT', txid: this.id });
    this.db.wal.flush(); // XLogFlush() — durability point
    this.mgr.commit(this.id);
  }

  // AbortTransaction() — write ROLLBACK to WAL (no fsync needed — nothing to preserve).
  rollback() {
    if (this.done) throw new Error('transaction has ended');
    this.done = true;
    this.db.wal.write({ type: 'ROLLBACK', txid: this.id });
    this.mgr.rollback(this.id);
  }
}

// ── Database ──────────────────────────────────────────────────────────────────

class Database {
  constructor(dir = '.') {
    this.buf     = new BufferPool(path.join(dir, DATA_FILE), MAX_BUF_PAGES);
    this.wal     = new WAL(path.join(dir, WAL_FILE));
    this.txnMgr  = new TxnMgr();
    this.nextPid = 1; // heap starts at page 1; page 0 is the catalog
    this._recover();  // replay WAL before accepting any connections — like pg's startup process
  }

  // StartupXLOG() redo phase — replay WAL records to reconstruct committed state.
  //
  // pg's startup process reads from the last checkpoint LSN forward.
  // We have no checkpoints, so we always replay the entire WAL — fine for a demo.
  // The two-pass approach mirrors pg: first find what committed, then redo their work.
  _recover() {
    const recs = this.wal.readAll();
    if (recs.length === 0) return; // fresh database — nothing to recover

    // Pass 1: rebuild committed set and highest txid seen.
    // This is like pg reading pg_xact (CLOG) pages to know which txids committed.
    let maxTxid = 0;
    for (const r of recs) {
      if (r.type === 'COMMIT') this.txnMgr.committed.add(r.txid);
      if (r.txid > maxTxid)   maxTxid = r.txid;
    }
    this.txnMgr.nextId = maxTxid + 1; // don't reuse txids — they live in old tuples

    // Pass 2: redo all committed inserts from WAL.
    // We truncate the heap back to just the catalog page, then replay, so the
    // heap file is in a consistent state regardless of what hit disk before the crash.
    // Uncommitted inserts are simply never replayed — their tuples don't reappear.
    this.buf.reset(); // truncate heap to catalog page only
    for (const r of recs) {
      if (r.type === 'INSERT' && this.txnMgr.committed.has(r.txid)) {
        const pid  = this._findPage(r.data);
        const page = this.buf.fetch(pid);
        appendTuple(page, r.txid, 0, r.data);
        this.buf.dirty(pid);
      }
    }
    this.buf.checkpoint(); // flush recovered pages to disk
    console.log(`  [WAL recovery] replayed ${recs.length} records — ` +
                `restored ${this.txnMgr.committed.size} committed txn(s), ` +
                `skipped all uncommitted`);
  }

  // RelationGetBufferForTuple() + FSM (Free Space Map) lookup.
  // pg maintains a dedicated FSM fork per relation to quickly find pages
  // with enough free space.  We do a simple linear scan — O(n) but readable.
  _findPage(dataStr) {
    const needed = TUPLE_HDR_SZ + Buffer.byteLength(dataStr, 'utf8');
    for (let pid = 1; pid < this.nextPid; pid++) {
      if (freeSpace(this.buf.fetch(pid)) >= needed) return pid;
    }
    // No page has room — extend the relation (smgrextend)
    return this.nextPid++;
  }

  begin() {
    const txn = this.txnMgr.begin(this);
    this.wal.write({ type: 'BEGIN', txid: txn.id });
    return txn;
  }

  close() { this.buf.close(); this.wal.close(); }
}

// ── Demo ──────────────────────────────────────────────────────────────────────
(function demo() {
  // Clean slate — remove any files left from a previous run
  for (const f of [DATA_FILE, WAL_FILE]) if (fs.existsSync(f)) fs.unlinkSync(f);

  console.log('╔══════════════════════════════════════════╗');
  console.log('║       TinyPG — Educational Demo           ║');
  console.log('╚══════════════════════════════════════════╝\n');

  // ── Scene 1: Insert rows in Transaction A ──────────────────────────────────
  console.log('▶  Scene 1 — Insert 5 rows in txA');
  let db  = new Database();
  const txA = db.begin();
  for (let i = 1; i <= 5; i++) txA.insert({ id: i, name: `row-${i}`, value: i * 10 });
  console.log(`   txA (id=${txA.id}) inserted 5 rows — NOT YET COMMITTED\n`);

  // ── Scene 2: MVCC — txB starts before txA commits ─────────────────────────
  // txB's snapshot was taken before txA committed, so txA's xmin is not in
  // txB's committed set → those tuples are invisible to txB.
  console.log('▶  Scene 2 — MVCC: txB starts BEFORE txA commits');
  const txB = db.begin();
  const before = txB.select();
  console.log(`   txB (id=${txB.id}) sees ${before.length} rows — expected 0`);
  console.log('   (txA\'s xmin not yet in committed set → invisible)\n');

  // ── Scene 3: Commit A; txB re-reads with fresh READ-COMMITTED snapshot ─────
  console.log('▶  Scene 3 — Commit txA; txB takes a fresh snapshot');
  txA.commit();
  const after = txB.select(); // new snapshot includes txA in committed set
  console.log(`   txA committed. txB now sees ${after.length} rows — expected 5`);
  console.log('   Rows: ' + after.map(r => r.name).join(', ') + '\n');
  txB.commit();

  // ── Scene 4: Crash mid-transaction ────────────────────────────────────────
  // txC inserts a row but the process "dies" before it commits.
  // The WAL will contain BEGIN + INSERT for txC but NO COMMIT record.
  // We do NOT call db.close() — pages in the buffer pool may never reach disk.
  console.log('▶  Scene 4 — Crash mid-transaction');
  const txC = db.begin();
  txC.insert({ id: 99, name: 'crash-row', value: 999 });
  console.log(`   txC (id=${txC.id}) inserted crash-row — process dies NOW (no commit, no close)`);
  console.log('   Simulating crash: dropping db reference without flushing...\n');
  db = null; // process death — buffer pool pages are lost, WAL on disk has the record

  // ── Scene 5: Restart + WAL recovery ───────────────────────────────────────
  // new Database() automatically calls _recover() which replays the WAL.
  // txC's INSERT is in the WAL but txC's COMMIT is not → it is not replayed.
  console.log('▶  Scene 5 — Restart & WAL replay (like pg startup process)');
  const db2       = new Database();  // _recover() fires here
  const txD       = db2.begin();
  const recovered = txD.select();
  console.log(`   After recovery: ${recovered.length} rows visible — expected 5`);
  console.log('   Rows: ' + recovered.map(r => r.name).join(', '));
  const hasCrashRow = recovered.some(r => r.name === 'crash-row');
  console.log(`   crash-row present: ${hasCrashRow} — expected false (txC never committed)\n`);
  txD.commit();

  // ── Assertions ─────────────────────────────────────────────────────────────
  console.log('── Assertions ─────────────────────────────────────────────');
  const assert = (desc, cond) => console.log(`  ${cond ? '✓' : '✗'} ${desc}`);
  assert('txB saw 0 rows before txA committed (MVCC)',       before.length === 0);
  assert('txB saw 5 rows after txA committed (READ COMMITTED)', after.length === 5);
  assert('WAL recovery restored exactly 5 rows',             recovered.length === 5);
  assert('crash-row excluded (uncommitted txn invisible)',    !hasCrashRow);
  console.log('\n✓  TinyPG behaves like a real ACID database\n');

  db2.close();
})();
