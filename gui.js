'use strict';
/**
 * gui.js — browser-based explorer for TinyPG (a tiny pgAdmin).
 *
 * Run:  node gui.js
 * Open: http://localhost:3000
 *
 * One Node HTTP server, no dependencies, no build step. The HTML/CSS/JS is
 * inlined at the bottom of this file so the whole thing ships as one process.
 *
 * Endpoints:
 *   GET  /             — the single-page UI
 *   POST /exec         — body: { sql }  →  { result, current_txid, error? }
 *   GET  /state        — current internals snapshot (used by the side panels
 *                        so they refresh on every command without a websocket)
 *
 * One implicit transaction per browser session: the server keeps a single
 * "current txn" reference, the same way cli.js does. If you want concurrency
 * (two transactions visible side-by-side, MVCC demos), open the page in two
 * browser windows — each tab gets its own session id via /session.
 */
const http = require('http');
const { Database, scanPage, freeOffset, PAGE_SIZE } = require('./tinypg.js');
const { parse } = require('./parser.js');

const PORT = process.env.PORT || 3000;
const db = new Database();

// Per-session current transaction. The session id lives in a cookie; this lets
// us demonstrate MVCC by opening two browser tabs — each gets its own txn and
// its own snapshot.
const sessions = new Map(); // sid → { txn: Txn|null }

function getSession(sid) {
  if (!sessions.has(sid)) sessions.set(sid, { txn: null });
  return sessions.get(sid);
}

// ── Internals snapshot — feeds the right-hand panels ─────────────────────────
function snapshotState() {
  const wal = db.wal.readAll().map(r => ({
    lsn: r.lsn, type: r.type, txid: r.txid, data: r.data, pid: r.pid, off: r.off,
  }));

  const pages = [];
  for (let pid = 1; pid < db.nextPid; pid++) {
    const page = db.buf.fetch(pid);
    const tuples = [];
    scanPage(page, (xmin, xmax, data, off) => {
      tuples.push({ off, xmin, xmax, data, alive: xmax === 0 });
    });
    pages.push({ pid, used: freeOffset(page), capacity: PAGE_SIZE, tuples });
  }

  const buffers = db.buf.lru.map(pid => ({
    pid, dirty: db.buf.frames.get(pid).dirty,
  }));

  return {
    wal, pages, buffers,
    txns: {
      active:    [...db.txnMgr.active.keys()],
      committed: [...db.txnMgr.committed].sort((a, b) => a - b),
      next:      db.txnMgr.nextId,
    },
  };
}

function rowMatches(pred, row) {
  if (!pred) return true;
  return row[pred.field] === pred.value || String(row[pred.field]) === String(pred.value);
}

// ── Execute one command — mirrors cli.js execute() ───────────────────────────
// Returns { ok, output, current_txid } or { ok:false, error }.
// The output is a string the UI prints into the result pane verbatim.
function execute(sql, session) {
  const cmd = parse(sql); // throws on parse error — caught by /exec handler

  switch (cmd.type) {
    case 'BEGIN':
      if (session.txn) return { output: `(noop) tx${session.txn.id} already active` };
      session.txn = db.begin();
      return { output: `BEGIN  tx${session.txn.id}` };

    case 'COMMIT':
      if (!session.txn) return { output: '(noop) no active transaction' };
      { const id = session.txn.id; session.txn.commit(); session.txn = null;
        return { output: `COMMIT tx${id}` }; }

    case 'ROLLBACK':
      if (!session.txn) return { output: '(noop) no active transaction' };
      { const id = session.txn.id; session.txn.rollback(); session.txn = null;
        return { output: `ROLLBACK tx${id}` }; }

    case 'INSERT': {
      if (!session.txn) session.txn = db.begin();
      session.txn.insert(cmd.row);
      return { output: `INSERT 1  (in tx${session.txn.id})` };
    }

    case 'SELECT': {
      if (!session.txn) session.txn = db.begin();
      const rows = session.txn.select(r => rowMatches(cmd.where, r));
      const body = rows.length
        ? rows.map(r => JSON.stringify(r)).join('\n') + `\n(${rows.length} rows)`
        : '(0 rows)';
      return { output: body };
    }

    case 'DELETE': {
      if (!session.txn) session.txn = db.begin();
      let n = 0;
      session.txn.delete(r => { if (rowMatches(cmd.where, r)) { n++; return true; } return false; });
      return { output: `DELETE ${n}  (in tx${session.txn.id})` };
    }

    case 'SHOW':
      // The UI already has side panels that show this live, but SHOW in the
      // query box is still useful for copy-pasteable output.
      return { output: JSON.stringify(snapshotState()[cmd.what.toLowerCase()] || snapshotState(), null, 2) };
  }
}

// ── HTTP server ───────────────────────────────────────────────────────────────
function readJson(req) {
  return new Promise((resolve, reject) => {
    let buf = '';
    req.on('data', c => { buf += c; if (buf.length > 1e6) { req.destroy(); reject(new Error('payload too big')); } });
    req.on('end',  () => { try { resolve(JSON.parse(buf || '{}')); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}

function sidFromCookie(req) {
  const c = req.headers.cookie || '';
  const m = /(?:^|;\s*)sid=([^;]+)/.exec(c);
  return m ? decodeURIComponent(m[1]) : null;
}

const server = http.createServer(async (req, res) => {
  // Issue a session cookie on first visit so each browser tab is its own session.
  let sid = sidFromCookie(req);
  const headers = { 'Content-Type': 'application/json' };
  if (!sid) {
    sid = 'sid-' + Math.random().toString(36).slice(2, 10) + '-' + Date.now().toString(36);
    headers['Set-Cookie'] = `sid=${sid}; Path=/; HttpOnly; SameSite=Lax`;
  }
  const session = getSession(sid);

  try {
    if (req.method === 'GET' && req.url === '/') {
      res.writeHead(200, { ...headers, 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(HTML);
    }

    if (req.method === 'GET' && req.url === '/state') {
      res.writeHead(200, headers);
      return res.end(JSON.stringify({
        state: snapshotState(),
        current_txid: session.txn ? session.txn.id : null,
      }));
    }

    if (req.method === 'POST' && req.url === '/exec') {
      const { sql } = await readJson(req);
      try {
        const result = execute(sql, session);
        res.writeHead(200, headers);
        return res.end(JSON.stringify({
          ok: true,
          output: result.output,
          current_txid: session.txn ? session.txn.id : null,
          state: snapshotState(),
        }));
      } catch (e) {
        res.writeHead(200, headers);
        return res.end(JSON.stringify({
          ok: false,
          error: e.message,
          current_txid: session.txn ? session.txn.id : null,
          state: snapshotState(),
        }));
      }
    }

    res.writeHead(404, headers);
    res.end(JSON.stringify({ error: 'not found' }));
  } catch (e) {
    res.writeHead(500, headers);
    res.end(JSON.stringify({ error: e.message }));
  }
});

process.on('SIGINT', () => {
  console.log('\nshutting down — flushing buffer pool...');
  for (const { txn } of sessions.values()) if (txn) try { txn.rollback(); } catch {}
  db.close();
  process.exit(0);
});

server.listen(PORT, () => {
  console.log(`TinyPG GUI ready  →  http://localhost:${PORT}`);
  console.log('Open in two browser windows to demo MVCC across sessions.');
  console.log('Ctrl-C to stop (flushes buffer pool cleanly).');
});

// ── Frontend ──────────────────────────────────────────────────────────────────
// Single self-contained HTML/CSS/JS. Three columns:
//   left  — query input + result output (the "psql" pane)
//   right — live panels: Pages, WAL, Buffer Pool, Transactions
// The right panels refresh after every /exec response so you can watch
// xmin/xmax appear, WAL grow, and the buffer pool fill up.
const HTML = String.raw`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>TinyPG Explorer</title>
<style>
  :root {
    --bg:#0e1116; --panel:#161b22; --panel2:#1c2330; --line:#2a313c;
    --text:#e6edf3; --dim:#7d8590; --accent:#58a6ff; --ok:#3fb950;
    --warn:#d29922; --err:#f85149; --mono:ui-monospace,Menlo,Consolas,monospace;
  }
  * { box-sizing: border-box; }
  html, body { margin:0; height:100%; background:var(--bg); color:var(--text); font:13px/1.5 system-ui,-apple-system,Segoe UI,sans-serif; }
  header { padding:10px 16px; background:var(--panel); border-bottom:1px solid var(--line); display:flex; align-items:center; gap:16px; }
  header h1 { margin:0; font-size:14px; font-weight:600; letter-spacing:.2px; }
  header .tag { font-family:var(--mono); font-size:11px; color:var(--dim); }
  header .tx { margin-left:auto; font-family:var(--mono); font-size:12px; padding:3px 8px; border-radius:4px; background:var(--panel2); border:1px solid var(--line); }
  header .tx.live { color:var(--warn); border-color:#5a4a1f; }
  main { display:grid; grid-template-columns: 1fr 1fr; height:calc(100vh - 47px); }
  .col { display:flex; flex-direction:column; min-height:0; }
  .col + .col { border-left:1px solid var(--line); }
  .pane { display:flex; flex-direction:column; min-height:0; border-bottom:1px solid var(--line); }
  .pane:last-child { border-bottom:none; }
  .pane > h2 { margin:0; padding:8px 14px; font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:.5px; color:var(--dim); background:var(--panel); border-bottom:1px solid var(--line); display:flex; gap:8px; }
  .pane > h2 .badge { background:var(--panel2); color:var(--text); padding:1px 6px; border-radius:3px; font-size:10px; }
  .pane > .body { flex:1; overflow:auto; padding:10px 14px; font-family:var(--mono); font-size:12px; }
  textarea#sql { flex:1; width:100%; background:#0a0d12; color:var(--text); border:none; outline:none; padding:14px; font-family:var(--mono); font-size:13px; resize:none; line-height:1.5; }
  .toolbar { display:flex; gap:8px; padding:8px 14px; background:var(--panel); border-top:1px solid var(--line); border-bottom:1px solid var(--line); align-items:center; flex-wrap:wrap; }
  button { background:var(--panel2); color:var(--text); border:1px solid var(--line); padding:5px 12px; border-radius:4px; font-size:12px; cursor:pointer; font-family:inherit; }
  button:hover { background:#252d3c; border-color:#3a4350; }
  button.primary { background:var(--accent); color:#0a0d12; border-color:var(--accent); font-weight:600; }
  button.primary:hover { background:#79b8ff; }
  .toolbar .hint { color:var(--dim); font-size:11px; margin-left:auto; }
  #output { white-space:pre-wrap; padding:14px; font-family:var(--mono); font-size:12px; color:var(--text); }
  #output .ok  { color:var(--ok); }
  #output .err { color:var(--err); }
  .tuple { padding:5px 8px; border:1px solid var(--line); border-radius:4px; margin-bottom:6px; background:var(--panel2); }
  .tuple.dead { opacity:.55; text-decoration:line-through wavy var(--err); }
  .tuple .meta { color:var(--dim); font-size:11px; }
  .page-card { margin-bottom:14px; border:1px solid var(--line); border-radius:5px; }
  .page-card h3 { margin:0; padding:6px 10px; font-size:12px; background:var(--panel2); border-bottom:1px solid var(--line); display:flex; }
  .page-card h3 .fill { margin-left:auto; color:var(--dim); font-weight:normal; }
  .page-card .tuples { padding:8px; }
  .wal-row { display:grid; grid-template-columns: 50px 80px 80px 1fr; gap:8px; padding:3px 0; border-bottom:1px dashed var(--line); }
  .wal-row .lsn { color:var(--dim); }
  .wal-row .type-COMMIT { color:var(--ok); }
  .wal-row .type-ROLLBACK { color:var(--err); }
  .wal-row .type-INSERT, .wal-row .type-BEGIN { color:var(--accent); }
  .wal-row .type-DELETE { color:var(--warn); }
  .buf-row { display:flex; gap:10px; padding:3px 0; }
  .buf-row .dirty { color:var(--err); font-weight:600; }
  .buf-row .clean { color:var(--ok); }
  .empty { color:var(--dim); font-style:italic; }
  .txn-list .label { color:var(--dim); display:inline-block; min-width:90px; }
  .txn-list .id { font-family:var(--mono); color:var(--accent); }
  kbd { background:var(--panel2); border:1px solid var(--line); border-bottom-width:2px; border-radius:3px; padding:1px 5px; font-size:11px; font-family:var(--mono); }
</style>
</head>
<body>

<header>
  <h1>TinyPG Explorer</h1>
  <span class="tag">localhost:3000</span>
  <span id="txn-pill" class="tx">no active txn</span>
</header>

<main>
  <!-- LEFT: query + results -->
  <div class="col">
    <div class="pane" style="flex:1;">
      <h2>Query</h2>
      <textarea id="sql" spellcheck="false" placeholder="-- try these
BEGIN
INSERT {&quot;id&quot;: 1, &quot;name&quot;: &quot;alice&quot;, &quot;age&quot;: 30}
INSERT {&quot;id&quot;: 2, &quot;name&quot;: &quot;bob&quot;, &quot;age&quot;: 25}
COMMIT
SELECT
SELECT WHERE id = 1
DELETE WHERE name = alice
SHOW WAL"></textarea>
      <div class="toolbar">
        <button id="run" class="primary">Run  <kbd>Ctrl</kbd>+<kbd>Enter</kbd></button>
        <button data-snippet="BEGIN">BEGIN</button>
        <button data-snippet="COMMIT">COMMIT</button>
        <button data-snippet="ROLLBACK">ROLLBACK</button>
        <button data-snippet="SELECT">SELECT</button>
        <button data-snippet='INSERT {"id": 1, "name": "row-1"}'>INSERT…</button>
        <span class="hint">One statement per Run. Cursor line is executed if multi-line.</span>
      </div>
    </div>
    <div class="pane" style="flex:1;">
      <h2>Result</h2>
      <div id="output" class="body empty">Result will appear here.</div>
    </div>
  </div>

  <!-- RIGHT: live internals -->
  <div class="col">
    <div class="pane" style="flex:1.4;">
      <h2>Heap Pages <span id="pages-count" class="badge">0</span></h2>
      <div id="pages" class="body"></div>
    </div>
    <div class="pane" style="flex:1;">
      <h2>WAL <span id="wal-count" class="badge">0</span></h2>
      <div id="wal" class="body"></div>
    </div>
    <div class="pane" style="flex:.5;">
      <h2>Buffer Pool <span id="buf-count" class="badge">0</span></h2>
      <div id="buf" class="body"></div>
    </div>
    <div class="pane" style="flex:.5;">
      <h2>Transactions</h2>
      <div id="txns" class="body txn-list"></div>
    </div>
  </div>
</main>

<script>
const $ = (id) => document.getElementById(id);
const out = $('output');
const pill = $('txn-pill');

function setTxn(id) {
  if (id) { pill.textContent = 'tx' + id + ' (uncommitted)'; pill.classList.add('live'); }
  else    { pill.textContent = 'no active txn';              pill.classList.remove('live'); }
}

function escapeHtml(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function renderState(s) {
  $('pages-count').textContent = s.pages.length;
  $('wal-count').textContent   = s.wal.length;
  $('buf-count').textContent   = s.buffers.length;

  // PAGES — every tuple, alive or dead
  if (s.pages.length === 0) {
    $('pages').innerHTML = '<div class="empty">No heap pages yet — page 0 is the catalog.</div>';
  } else {
    $('pages').innerHTML = s.pages.map(p => {
      const tuples = p.tuples.length
        ? p.tuples.map(t => '<div class="tuple ' + (t.alive ? '' : 'dead') + '">' +
            '<div class="meta">off=' + t.off + ' &nbsp; xmin=tx' + t.xmin +
            ' &nbsp; xmax=' + (t.xmax === 0 ? '<span style="color:var(--ok)">alive</span>' : 'tx' + t.xmax) + '</div>' +
            '<div>' + escapeHtml(t.data) + '</div></div>').join('')
        : '<div class="empty">(empty page)</div>';
      return '<div class="page-card"><h3>Page ' + p.pid +
             '<span class="fill">' + p.used + ' / ' + p.capacity + ' B</span></h3>' +
             '<div class="tuples">' + tuples + '</div></div>';
    }).join('');
  }

  // WAL
  if (s.wal.length === 0) {
    $('wal').innerHTML = '<div class="empty">(empty)</div>';
  } else {
    $('wal').innerHTML = s.wal.map(r => {
      const extra = r.data !== undefined ? escapeHtml(r.data)
                  : r.pid  !== undefined ? 'pid=' + r.pid + ' off=' + r.off
                  : '';
      return '<div class="wal-row">' +
             '<span class="lsn">lsn ' + r.lsn + '</span>' +
             '<span class="type-' + r.type + '">' + r.type + '</span>' +
             '<span>tx' + r.txid + '</span>' +
             '<span>' + extra + '</span></div>';
    }).join('');
  }

  // BUFFER POOL
  if (s.buffers.length === 0) {
    $('buf').innerHTML = '<div class="empty">(empty)</div>';
  } else {
    $('buf').innerHTML = '<div style="color:var(--dim); font-size:11px; margin-bottom:6px;">LRU order (oldest → newest):</div>' +
      s.buffers.map(b =>
        '<div class="buf-row">page ' + b.pid + ' &nbsp; <span class="' + (b.dirty ? 'dirty' : 'clean') + '">' +
        (b.dirty ? 'DIRTY' : 'clean') + '</span></div>').join('');
  }

  // TXNS
  const a = s.txns.active.length ? s.txns.active.map(i => '<span class="id">tx' + i + '</span>').join(', ') : '<span class="empty">none</span>';
  const c = s.txns.committed.length ? s.txns.committed.map(i => '<span class="id">tx' + i + '</span>').join(', ') : '<span class="empty">none</span>';
  $('txns').innerHTML =
    '<div><span class="label">Active:</span>'    + a + '</div>' +
    '<div><span class="label">Committed:</span>' + c + '</div>' +
    '<div><span class="label">Next txid:</span> <span class="id">tx' + s.txns.next + '</span></div>';
}

function currentStatement() {
  const ta = $('sql');
  const v = ta.value;
  // Use the cursor's line if the buffer has multiple non-empty lines; else
  // grab the first non-empty line. Keeps the "one statement at a time" feel
  // even when the textarea contains a script.
  const lines = v.split(/\r?\n/);
  if (lines.filter(l => l.trim()).length <= 1) {
    return v.trim();
  }
  const upto = v.slice(0, ta.selectionStart);
  const lineIdx = upto.split(/\r?\n/).length - 1;
  for (let i = lineIdx; i >= 0; i--) if (lines[i].trim()) return lines[i].trim();
  for (let i = lineIdx + 1; i < lines.length; i++) if (lines[i].trim()) return lines[i].trim();
  return '';
}

async function run() {
  const sql = currentStatement();
  if (!sql) return;
  out.classList.remove('empty');
  out.innerHTML = '<span class="dim">running…</span>';
  try {
    const r = await fetch('/exec', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql }),
    });
    const j = await r.json();
    if (j.ok) {
      out.innerHTML = '<span class="ok">' + escapeHtml(sql) + '</span>\n' + escapeHtml(j.output);
    } else {
      out.innerHTML = '<span class="err">' + escapeHtml(sql) + '\n! ' + escapeHtml(j.error) + '</span>';
    }
    setTxn(j.current_txid);
    if (j.state) renderState(j.state);
  } catch (e) {
    out.innerHTML = '<span class="err">network error: ' + escapeHtml(e.message) + '</span>';
  }
}

$('run').addEventListener('click', run);
$('sql').addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); run(); }
});
document.querySelectorAll('button[data-snippet]').forEach(b => {
  b.addEventListener('click', () => {
    const ta = $('sql');
    const txt = b.dataset.snippet;
    const cur = ta.value;
    ta.value = cur && !cur.endsWith('\n') ? cur + '\n' + txt : cur + txt;
    ta.focus();
    ta.selectionStart = ta.selectionEnd = ta.value.length;
  });
});

// Initial state
fetch('/state').then(r => r.json()).then(j => {
  setTxn(j.current_txid);
  renderState(j.state);
});
</script>
</body>
</html>`;
