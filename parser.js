'use strict';
/**
 * parser.js — tiny command parser shared by cli.js and gui.js.
 *
 * Not a real SQL parser — recognises just enough of a SQL-ish dialect to drive
 * the engine. Keeping it here (instead of inside cli.js) means the GUI shows
 * the exact same error messages as the shell.
 *
 * Grammar:
 *   BEGIN | COMMIT | ROLLBACK
 *
 *   CREATE TABLE <name>
 *   DROP   TABLE <name>
 *   CREATE INDEX <name> ON <table> ( <field> )
 *   DROP   INDEX <name>
 *
 *   INSERT [INTO <table> [VALUES]] <json-object>
 *   SELECT [* FROM <table>] [WHERE <field> = <value>]
 *   DELETE [FROM <table>] WHERE <field> = <value>
 *
 *   SHOW (TABLES | INDEXES | WAL | PAGES | BUFFERS | TXNS)
 *
 * Values can be unquoted (treated as string|number) or single/double-quoted.
 * Identifiers (table/index/field names) are [A-Za-z_][A-Za-z0-9_]*.
 */

const ID = /[A-Za-z_][A-Za-z0-9_]*/;

function parseWhere(rest) {
  const s = rest.trim();
  if (!s) return null;
  const m = new RegExp(`^where\\s+(${ID.source})\\s*=\\s*(.+)$`, 'i').exec(s);
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

    // ── DDL ───────────────────────────────────────────────────────────────
    case 'CREATE': {
      const tbl = new RegExp(`^table\\s+(${ID.source})\\s*$`, 'i').exec(rest);
      if (tbl) return { type: 'CREATE_TABLE', name: tbl[1] };

      // CREATE INDEX <name> ON <table> ( <field> )
      //   also accept:  CREATE INDEX <name> ON <table>(<field>)
      //                 CREATE INDEX <name> ON <table> <field>
      const idx = new RegExp(
        `^index\\s+(${ID.source})\\s+on\\s+(${ID.source})\\s*[\\s(]\\s*(${ID.source})\\s*\\)?\\s*$`,
        'i'
      ).exec(rest);
      if (idx) return { type: 'CREATE_INDEX', name: idx[1], table: idx[2], field: idx[3] };
      throw new Error('expected CREATE TABLE <name>  or  CREATE INDEX <name> ON <table>(<field>)');
    }

    case 'DROP': {
      const tbl = new RegExp(`^table\\s+(${ID.source})\\s*$`, 'i').exec(rest);
      if (tbl) return { type: 'DROP_TABLE', name: tbl[1] };
      const idx = new RegExp(`^index\\s+(${ID.source})\\s*$`, 'i').exec(rest);
      if (idx) return { type: 'DROP_INDEX', name: idx[1] };
      throw new Error('expected DROP TABLE <name>  or  DROP INDEX <name>');
    }

    // ── DML ───────────────────────────────────────────────────────────────
    case 'INSERT': {
      // Accept any of:
      //   INSERT {json}
      //   INSERT INTO heap {json}
      //   INSERT INTO heap VALUES {json}
      let table = null;
      let body  = rest;
      const into = new RegExp(`^into\\s+(${ID.source})\\s+(values\\s+)?(.+)$`, 'is').exec(rest);
      if (into) { table = into[1]; body = into[3]; }
      let row;
      try { row = JSON.parse(body); }
      catch (e) { throw new Error('INSERT expects a JSON object: ' + e.message); }
      if (!row || typeof row !== 'object' || Array.isArray(row)) {
        throw new Error('INSERT row must be a JSON object');
      }
      return { type: 'INSERT', table, row };
    }

    case 'SELECT': {
      // Accept: SELECT  |  SELECT WHERE ...  |  SELECT * FROM tbl [WHERE ...]
      let table = null;
      let body  = rest;
      const from = new RegExp(`^\\*?\\s*from\\s+(${ID.source})\\s*(.*)$`, 'is').exec(rest);
      if (from) { table = from[1]; body = from[2]; }
      else      { body = rest.replace(/^\*\s*/, ''); }
      return { type: 'SELECT', table, where: parseWhere(body) };
    }

    case 'DELETE': {
      let table = null;
      let body  = rest;
      const from = new RegExp(`^from\\s+(${ID.source})\\s*(.*)$`, 'is').exec(rest);
      if (from) { table = from[1]; body = from[2]; }
      const where = parseWhere(body);
      if (!where) throw new Error('DELETE requires a WHERE clause (refuse to wipe table)');
      return { type: 'DELETE', table, where };
    }

    case 'SHOW': {
      const what = rest.toUpperCase();
      const valid = ['TABLES', 'INDEXES', 'WAL', 'PAGES', 'BUFFERS', 'TXNS'];
      if (!valid.includes(what)) {
        throw new Error('SHOW target must be one of: ' + valid.join(', '));
      }
      return { type: 'SHOW', what };
    }

    default:
      throw new Error(`unknown command: ${verb}`);
  }
}

module.exports = { parse };
