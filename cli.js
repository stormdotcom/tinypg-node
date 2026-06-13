'use strict';
/**
 * cli.js — interactive shell for TinyPG (like psql for real Postgres).
 *
 * Run: node cli.js
 *
 * Commands (case-insensitive, one per line):
 *   BEGIN                         start a new transaction
 *   COMMIT                        commit the current transaction
 *   ROLLBACK                      abort the current transaction
 *   INSERT {json}                 add a row (raw JSON object)
 *   SELECT [WHERE field=value]    read visible rows
 *   DELETE WHERE field=value      mark matching rows deleted
 *   SHOW WAL                      dump the write-ahead log
 *   SHOW PAGES                    dump every tuple on every page (xmin/xmax)
 *   SHOW BUFFERS                  list pages in the buffer pool + dirty flags
 *   SHOW TXNS                     active + committed transaction ids
 *   .help                         show this help
 *   .exit                         close db and quit
 *
 * The current transaction is auto-started on the first mutation if none is
 * active — matches psql's autocommit feel. Reads also start an implicit
 * transaction so SELECT works without typing BEGIN first.
 */
const readline = require('readline');
const {
  Database, scanPage, freeOffset, PAGE_SIZE,
} = require('./tinypg.js');
const { parse } = require('./parser.js');

const db = new Database();
let txn = null;

function ensureTxn() {
  if (!txn) txn = db.begin();
  return txn;
}

// ── SHOW commands — peek at internal state for "see execution" workflow ───
// These are the educational payoff: students can watch xmin/xmax change,
// see WAL records accumulate, watch the buffer pool fill up and evict.
function showWAL() {
  const recs = db.wal.readAll();
  if (recs.length === 0) return console.log('(empty WAL)');
  for (const r of recs) console.log(`  lsn=${r.lsn}  ${r.type.padEnd(8)} txid=${r.txid}` +
                                    (r.data ? `  data=${r.data}` : '') +
                                    (r.pid !== undefined ? `  pid=${r.pid} off=${r.off}` : ''));
}

function showPages() {
  if (db.nextPid === 1) return console.log('(no heap pages yet — page 0 is the catalog)');
  for (let pid = 1; pid < db.nextPid; pid++) {
    const page = db.buf.fetch(pid);
    const used = freeOffset(page);
    console.log(`  page ${pid}  ${used}/${PAGE_SIZE} bytes used`);
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

function rowMatches(pred, row) {
  if (!pred) return true;
  return row[pred.field] === pred.value || String(row[pred.field]) === String(pred.value);
}

const HELP = `
Commands:
  BEGIN | COMMIT | ROLLBACK
  INSERT <json-object>
  SELECT [WHERE field=value]
  DELETE WHERE field=value
  SHOW WAL | SHOW PAGES | SHOW BUFFERS | SHOW TXNS
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
      case 'BEGIN':
        if (txn) return console.log(`  ! tx${txn.id} already active — COMMIT or ROLLBACK first`);
        txn = db.begin();
        return console.log(`  BEGIN  (tx${txn.id})`);

      case 'COMMIT':
        if (!txn) return console.log('  ! no active transaction');
        { const id = txn.id; txn.commit(); txn = null; console.log(`  COMMIT (tx${id})`); }
        return;

      case 'ROLLBACK':
        if (!txn) return console.log('  ! no active transaction');
        { const id = txn.id; txn.rollback(); txn = null; console.log(`  ROLLBACK (tx${id})`); }
        return;

      case 'INSERT':
        ensureTxn().insert(cmd.row);
        return console.log(`  INSERT 1  (tx${txn.id})`);

      case 'SELECT': {
        const t = ensureTxn();
        const rows = t.select(r => rowMatches(cmd.where, r));
        if (rows.length === 0) console.log('  (0 rows)');
        else { for (const r of rows) console.log('  ' + JSON.stringify(r)); console.log(`  (${rows.length} row${rows.length === 1 ? '' : 's'})`); }
        return;
      }

      case 'DELETE': {
        const t = ensureTxn();
        let count = 0;
        t.delete(r => { if (rowMatches(cmd.where, r)) { count++; return true; } return false; });
        return console.log(`  DELETE ${count}  (tx${t.id})`);
      }

      case 'SHOW':
        if (cmd.what === 'WAL')     return showWAL();
        if (cmd.what === 'PAGES')   return showPages();
        if (cmd.what === 'BUFFERS') return showBuffers();
        if (cmd.what === 'TXNS')    return showTxns();
        return console.log('  ! unknown SHOW target — try WAL | PAGES | BUFFERS | TXNS');
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
