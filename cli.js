'use strict';
/**
 * cli.js — interactive shell for TinyPG (like psql for real Postgres).
 *
 * Run: node cli.js
 *
 * Commands (case-insensitive, one per line):
 *   BEGIN | COMMIT | ROLLBACK
 *   CREATE TABLE <name>                 DROP TABLE <name>
 *   CREATE INDEX <name> ON <tbl>(<col>) DROP INDEX <name>
 *   INSERT [INTO <tbl> [VALUES]] <json>
 *   SELECT [* FROM <tbl>] [WHERE field = value]
 *   DELETE [FROM <tbl>] WHERE field = value
 *   SHOW TABLES | INDEXES | WAL | PAGES | BUFFERS | TXNS
 *   .help | .exit
 *
 * The default table is "main" — used whenever a command omits a table name,
 * so the original `INSERT {...}` / `SELECT` / `DELETE WHERE ...` flows still
 * work without typing CREATE TABLE first.
 */
const readline = require('readline');
const {
  Database, scanPage, freeOffset, PAGE_SIZE,
} = require('./tinypg.js');
const { parse } = require('./parser.js');

const DEFAULT_TABLE = 'main';

const db = new Database();
let txn = null;

function ensureTxn() {
  if (!txn) txn = db.begin();
  return txn;
}

// ── SHOW commands — peek at internal state for the "see execution" workflow ─
function showWAL() {
  const recs = db.wal.readAll();
  if (recs.length === 0) return console.log('(empty WAL)');
  for (const r of recs) {
    const tail =
        r.data  !== undefined ? `  data=${r.data}`
      : r.pid   !== undefined ? `  pid=${r.pid} off=${r.off}`
      : r.table !== undefined ? `  table=${r.table}${r.field ? ` field=${r.field}` : ''}`
      : '';
    console.log(`  lsn=${r.lsn}  ${r.type.padEnd(13)} ` +
                `txid=${r.txid !== undefined ? r.txid : '-'}${tail}`);
  }
}

function showPages() {
  const tables = db.listTables();
  const allPids = new Set();
  for (const t of tables) for (const p of t.pages) allPids.add(p);
  if (allPids.size === 0) return console.log('(no heap pages allocated)');
  const sorted = [...allPids].sort((a, b) => a - b);
  for (const pid of sorted) {
    const page = db.buf.fetch(pid);
    const owner = tables.find(t => t.pages.includes(pid));
    const used = freeOffset(page);
    console.log(`  page ${pid}  table=${owner ? owner.name : '?'}  ${used}/${PAGE_SIZE} bytes used`);
    scanPage(page, (xmin, xmax, data, off) => {
      const status = xmax === 0 ? 'alive' : `deleted by tx${xmax}`;
      console.log(`    off=${off}  xmin=${xmin}  xmax=${xmax}  [${status}]  ${data}`);
    });
  }
}

function showBuffers() {
  if (db.buf.frames.size === 0) return console.log('(buffer pool is empty)');
  console.log(`  ${db.buf.frames.size}/${db.buf.maxPages} frames in use, LRU order:`);
  for (const pid of db.buf.lru) {
    const e = db.buf.frames.get(pid);
    console.log(`    page ${pid}  ${e.dirty ? 'DIRTY' : 'clean'}`);
  }
}

function showTxns() {
  console.log(`  current : ${txn ? `tx${txn.id} (uncommitted)` : '(none)'}`);
  const active = [...db.txnMgr.active.keys()];
  console.log(`  active  : ${active.length ? active.map(i => 'tx' + i).join(', ') : '(none)'}`);
  const cmt = [...db.txnMgr.committed].sort((a, b) => a - b);
  console.log(`  committed: ${cmt.length ? cmt.map(i => 'tx' + i).join(', ') : '(none)'}`);
  console.log(`  next txid: ${db.txnMgr.nextId}`);
}

function showTables() {
  const ts = db.listTables();
  if (ts.length === 0) return console.log('(no tables)');
  console.log('  name             pages              rows');
  console.log('  ─────────────────────────────────────────');
  for (const t of ts) {
    console.log(`  ${t.name.padEnd(16)} [${t.pages.join(', ').padEnd(16)}] ${t.rowCount}`);
  }
}

function showIndexes() {
  const is = db.listIndexes();
  if (is.length === 0) return console.log('(no indexes)');
  console.log('  name             table          field          entries');
  console.log('  ──────────────────────────────────────────────────────');
  for (const i of is) {
    console.log(`  ${i.name.padEnd(16)} ${i.table.padEnd(14)} ${i.field.padEnd(14)} ${i.entryCount}`);
  }
}

const HELP = `
Commands:
  BEGIN | COMMIT | ROLLBACK
  CREATE TABLE <name>                 DROP TABLE <name>
  CREATE INDEX <name> ON <tbl>(<col>) DROP INDEX <name>
  INSERT [INTO <tbl>] <json-object>
  SELECT [* FROM <tbl>] [WHERE field=value]
  DELETE [FROM <tbl>] WHERE field=value
  SHOW TABLES | INDEXES | WAL | PAGES | BUFFERS | TXNS
  .help | .exit
`;

function execute(line) {
  const trimmed = line.trim();
  if (!trimmed) return;
  if (trimmed === '.exit') return shutdown();
  if (trimmed === '.help') return console.log(HELP);

  let cmd;
  try { cmd = parse(trimmed); }
  catch (e) { return console.log('  ! parse error: ' + e.message); }

  try {
    switch (cmd.type) {
      // ── Transaction control ─────────────────────────────────────────
      case 'BEGIN':
        if (txn) return console.log(`  ! tx${txn.id} already active — COMMIT or ROLLBACK first`);
        txn = db.begin();
        return console.log(`  BEGIN  (tx${txn.id})`);

      case 'COMMIT':
        if (!txn) return console.log('  ! no active transaction');
        { const id = txn.id; txn.commit(); txn = null;
          return console.log(`  COMMIT (tx${id})`); }

      case 'ROLLBACK':
        if (!txn) return console.log('  ! no active transaction');
        { const id = txn.id; txn.rollback(); txn = null;
          return console.log(`  ROLLBACK (tx${id})`); }

      // ── DDL ─────────────────────────────────────────────────────────
      case 'CREATE_TABLE':
        db.createTable(cmd.name);
        return console.log(`  CREATE TABLE ${cmd.name}`);

      case 'DROP_TABLE':
        db.dropTable(cmd.name);
        return console.log(`  DROP TABLE ${cmd.name}`);

      case 'CREATE_INDEX':
        db.createIndex(cmd.name, cmd.table, cmd.field);
        return console.log(`  CREATE INDEX ${cmd.name} ON ${cmd.table}(${cmd.field})`);

      case 'DROP_INDEX':
        db.dropIndex(cmd.name);
        return console.log(`  DROP INDEX ${cmd.name}`);

      // ── DML ─────────────────────────────────────────────────────────
      case 'INSERT': {
        const t = cmd.table || DEFAULT_TABLE;
        ensureTxn().insert(t, cmd.row);
        return console.log(`  INSERT 1  into ${t}  (tx${txn.id})`);
      }

      case 'SELECT': {
        const table = cmd.table || DEFAULT_TABLE;
        const t = ensureTxn();
        let rows, plan;
        if (cmd.where) {
          ({ rows, plan } = t.lookup(table, cmd.where.field, cmd.where.value));
        } else {
          rows = t.select(table);
          plan = { type: 'seq', pagesScanned: (db.tables.get(table) || { pages: [] }).pages.length };
        }
        if (rows.length === 0) console.log('  (0 rows)');
        else { for (const r of rows) console.log('  ' + JSON.stringify(r));
               console.log(`  (${rows.length} row${rows.length === 1 ? '' : 's'})`); }
        const planStr = plan.type === 'index'
          ? `index ${plan.name}`
          : `seq scan (${plan.pagesScanned} page${plan.pagesScanned === 1 ? '' : 's'})`;
        return console.log(`  -- plan: ${planStr} on ${table}`);
      }

      case 'DELETE': {
        const table = cmd.table || DEFAULT_TABLE;
        const t = ensureTxn();
        let count = 0;
        t.delete(table, r =>
          (r[cmd.where.field] === cmd.where.value || String(r[cmd.where.field]) === String(cmd.where.value))
            ? (count++, true) : false);
        return console.log(`  DELETE ${count}  from ${table}  (tx${t.id})`);
      }

      // ── Inspection ──────────────────────────────────────────────────
      case 'SHOW':
        if (cmd.what === 'TABLES')  return showTables();
        if (cmd.what === 'INDEXES') return showIndexes();
        if (cmd.what === 'WAL')     return showWAL();
        if (cmd.what === 'PAGES')   return showPages();
        if (cmd.what === 'BUFFERS') return showBuffers();
        if (cmd.what === 'TXNS')    return showTxns();
        return;
    }
  } catch (e) {
    console.log('  ! ' + e.message);
  }
}

function shutdown() {
  if (txn) { console.log(`  auto-rollback tx${txn.id}`); txn.rollback(); }
  db.close();
  console.log('bye.');
  process.exit(0);
}

console.log('TinyPG shell — type .help for commands, .exit to quit.');
const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: 'tinypg> ' });
rl.prompt();
rl.on('line', l => { execute(l); rl.prompt(); });
rl.on('close', shutdown);
