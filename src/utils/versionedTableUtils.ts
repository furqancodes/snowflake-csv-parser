// versionedTableUtils.ts

import { Connection } from "snowflake-sdk";
import { normalizeType, runQuery } from "../helpers/helper";
import { ColumnDefinition } from "../types";

/**
 * Finds or creates a versioned table with the desired columns.
 *
 * 1) Looks for tables named either <baseName> or <baseName>_vX, with the highest X.
 * 2) If that table's columns strictly match `desiredColumns`, returns it.
 * 3) Otherwise, creates a new table <baseName>_v(X+1> with the correct columns.
 * 4) Returns the final table name.
 */
export async function getOrCreateVersionedTable(
  connection: Connection,
  schemaName: string,
  baseName: string,
  desiredColumns: ColumnDefinition[]
): Promise<string> {
  // 1) Get existing tables that start with baseName
  const tableNames = await getMatchingTableNames(
    connection,
    schemaName,
    baseName
  );

  // 2) Determine highest version
  let maxVersion = -1;
  let bestMatchTable = null;

  for (const t of tableNames) {
    const version = parseVersion(baseName, t); // e.g. "users_v2" -> 2, "users" -> 0
    if (version > maxVersion) {
      maxVersion = version;
      bestMatchTable = t;
    }
  }

  // If no table found, maxVersion is -1 => create <baseName> as first version
  if (maxVersion === -1) {
    await createVersionedTable(
      connection,
      schemaName,
      baseName,
      desiredColumns
    );
    return baseName;
  }

  // If we did find a table, check columns
  if (bestMatchTable) {
    const columnsMatch = await doColumnsMatch(
      connection,
      schemaName,
      bestMatchTable,
      desiredColumns
    );
    if (columnsMatch) {
      return bestMatchTable; // reuse existing
    }
  }

  // Otherwise create a new version
  const newVersion = maxVersion + 1;
  const newTableName = `${baseName}_v${newVersion}`;
  await createVersionedTable(
    connection,
    schemaName,
    newTableName,
    desiredColumns
  );
  return newTableName;
}

/** Gets all table names in the schema that start with baseName. */
async function getMatchingTableNames(
  connection: Connection,
  schemaName: string,
  baseName: string
): Promise<string[]> {
  const sql = `
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '${schemaName}'
      AND TABLE_NAME ILIKE '${baseName}%'
    ORDER BY TABLE_NAME
  `;
  const rows = await runQuery(connection, sql);
  return rows.map((r: any) => r.TABLE_NAME);
}

/**
 * If tableName == baseName, version=0
 * If tableName == baseName_vX, version=X
 * Otherwise -1 if not recognized
 */
function parseVersion(baseName: string, tableName: string): number {
  if (tableName.toLowerCase() === baseName.toLowerCase()) {
    return 0; // unversioned
  }
  const regex = new RegExp(`^${baseName}_v(\\d+)$`, "i");
  const match = tableName.match(regex);
  if (!match) return -1;
  return parseInt(match[1], 10);
}

/** Compares columns in a table with the desiredColumns array. */
async function doColumnsMatch(
  connection: Connection,
  schemaName: string,
  tableName: string,
  desiredColumns: ColumnDefinition[]
): Promise<boolean> {
  const sql = `
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '${schemaName}'
      AND TABLE_NAME = '${tableName.toUpperCase()}'
    ORDER BY ORDINAL_POSITION
  `;
  const rows = await runQuery(connection, sql);
  if (rows.length !== desiredColumns.length) {
    return false;
  }

  for (let i = 0; i < rows.length; i++) {
    const actualName = rows[i].COLUMN_NAME.toLowerCase();
    const actualType = normalizeType(rows[i].DATA_TYPE);
    const desiredName = desiredColumns[i].name.toLowerCase();
    const desiredType = normalizeType(desiredColumns[i].type);

    if (actualName !== desiredName || actualType !== desiredType) {
      return false;
    }
  }
  return true;
}

/** Builds and runs CREATE TABLE with the given columns. */
async function createVersionedTable(
  connection: Connection,
  schemaName: string,
  tableName: string,
  desiredColumns: ColumnDefinition[]
): Promise<void> {
  const colDefs = desiredColumns
    .map((c) => `${c.name} ${c.type}`)
    .join(",\n  ");
  const sql = `
    CREATE TABLE ${schemaName}.${tableName} (
      ${colDefs}
    )
  `;
  await runQuery(connection, sql);
}
