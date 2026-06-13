'use strict';
/**
 * parser.js — tiny command parser shared by cli.js and gui.js.
 *
 * Not a real SQL parser — recognises just enough of a SQL-ish dialect to drive
 * the engine. Keeping it here (instead of inside cli.js) means the GUI shows
 * the exact same error messages as the shell.
 *
 * Grammar:
 *   BEGIN
 *   COMMIT
 *   ROLLBACK
 *   INSERT <json>                             — json must be an object literal
 *   SELECT [ WHERE <field> = <value> ]
 *   DELETE WHERE <field> = <value>
 *   SHOW (WAL | PAGES | BUFFERS | TXNS)
 *
 * Values can be unquoted (treated as string|number) or single/double-quoted.
 */

function parseWhere(rest) {
  // rest may be empty (no WHERE), or "WHERE field=value" / "WHERE field = value"
  const s = rest.trim();
  if (!s) return null;
  const m = /^where\s+(\w+)\s*=\s*(.+)$/i.exec(s);
  if (!m) throw new Error('expected WHERE <field>=<value>');
  let value = m[2].trim();
  const q = value[0];
  if ((q === '"' || q === "'") && value[value.length - 1] === q) {
    value = value.slice(1, -1);
  } else if (/^-?\d+(\.\d+)?$/.test(value)) {
    value = Number(value);
  }
  return { field: m[1], value };
}

function parse(line) {
  const m = /^(\w+)(\s+(.*))?$/s.exec(line.trim());
  if (!m) throw new Error('empty command');
  const verb = m[1].toUpperCase();
  const rest = (m[3] || '').trim();

  switch (verb) {
    case 'BEGIN':
    case 'COMMIT':
    case 'ROLLBACK':
      if (rest) throw new Error(`${verb} takes no arguments`);
      return { type: verb };

    case 'INSERT': {
      // Accept "INSERT {json}" or "INSERT INTO heap VALUES {json}" or "INSERT INTO heap {json}"
      let json = rest;
      const into = /^into\s+\w+\s+(values\s+)?(.+)$/is.exec(rest);
      if (into) json = into[2];
      let row;
      try { row = JSON.parse(json); }
      catch (e) { throw new Error('INSERT expects a JSON object: ' + e.message); }
      if (!row || typeof row !== 'object' || Array.isArray(row)) {
        throw new Error('INSERT row must be a JSON object');
      }
      return { type: 'INSERT', row };
    }

    case 'SELECT': {
      // Accept "SELECT", "SELECT WHERE ...", "SELECT * FROM heap", "SELECT * FROM heap WHERE ..."
      const stripped = rest.replace(/^\*\s+from\s+\w+\s*/i, '').replace(/^\*\s*/, '');
      return { type: 'SELECT', where: parseWhere(stripped) };
    }

    case 'DELETE': {
      const stripped = rest.replace(/^from\s+\w+\s*/i, '');
      const where = parseWhere(stripped);
      if (!where) throw new Error('DELETE requires a WHERE clause (refuse to wipe table)');
      return { type: 'DELETE', where };
    }

    case 'SHOW': {
      const what = rest.toUpperCase();
      if (!['WAL', 'PAGES', 'BUFFERS', 'TXNS'].includes(what)) {
        throw new Error('SHOW target must be one of: WAL, PAGES, BUFFERS, TXNS');
      }
      return { type: 'SHOW', what };
    }

    default:
      throw new Error(`unknown command: ${verb}`);
  }
}

module.exports = { parse };
