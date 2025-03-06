import * as snowflake from "snowflake-sdk";
import { config } from "../src/config";
import { runQuery } from "../src/helpers/helper";
import { getOrCreateVersionedTable } from "../src/utils/versionedTableUtils";
import { ColumnDefinition } from "../src/types";

describe("Versioned Table Utils", () => {
  jest.setTimeout(60000);

  const TEST_DB = "TEST_VERSION_DB";
  const TEST_SCHEMA = "PUBLIC";
  let connection: snowflake.Connection;

  beforeAll(async () => {
    connection = snowflake.createConnection({
      account: config.snowflakeAccount,
      username: config.snowflakeUsername,
      password: config.snowflakePassword,
      warehouse: config.snowflakeWarehouse,
      database: TEST_DB,
      schema: TEST_SCHEMA,
    });

    await new Promise<void>((resolve, reject) => {
      connection.connect((err) => {
        if (err) return reject(err);
        resolve();
      });
    });

    await runQuery(connection, `DROP DATABASE IF EXISTS ${TEST_DB}`);
    await runQuery(connection, `CREATE DATABASE ${TEST_DB}`);
    await runQuery(connection, `USE DATABASE ${TEST_DB}`);
  });

  afterAll(async () => {
    await runQuery(connection, `DROP DATABASE IF EXISTS ${TEST_DB}`);
    if (connection) {
      await new Promise<void>((resolve) => {
        connection.destroy(() => {
          console.log("Snowflake connection closed.");
          resolve();
        });
      });
    }
  });

  afterEach(async () => {});

  it("1) Creates a table if no versioned table exists", async () => {
    const desiredColumns: ColumnDefinition[] = [
      { name: "id", type: "STRING" },
      { name: "name", type: "STRING" },
    ];

    const tableName = await getOrCreateVersionedTable(
      connection,
      TEST_SCHEMA,
      "users",
      desiredColumns
    );
    expect(tableName).toBe("users");

    const rows = await runQuery(
      connection,
      `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_NAME = 'USERS' AND TABLE_SCHEMA = '${TEST_SCHEMA}'`
    );
    expect(rows.length).toBe(1);
  });

  it("2) Reuses the table if it already matches columns", async () => {
    const desiredColumns: ColumnDefinition[] = [
      { name: "id", type: "STRING" },
      { name: "name", type: "STRING" },
    ];

    const tableName = await getOrCreateVersionedTable(
      connection,
      TEST_SCHEMA,
      "users",
      desiredColumns
    );
    expect(tableName).toBe("USERS");

    const rows = await runQuery(
      connection,
      `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = '${TEST_SCHEMA}' AND TABLE_NAME = 'USERS_V1'`
    );
    expect(rows.length).toBe(0);
  });

  it("3) Creates new version if columns differ", async () => {
    const newColumns: ColumnDefinition[] = [
      { name: "id", type: "STRING" },
      { name: "name", type: "STRING" },
      { name: "age", type: "INTEGER" },
    ];

    const tableName = await getOrCreateVersionedTable(
      connection,
      TEST_SCHEMA,
      "users",
      newColumns
    );

    expect(tableName).toBe("users_v1");

    const rows = await runQuery(
      connection,
      `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = '${TEST_SCHEMA}' AND TABLE_NAME = 'USERS_V1'`
    );
    expect(rows.length).toBe(1);

    const colRows = await runQuery(
      connection,
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = '${TEST_SCHEMA}' AND TABLE_NAME = 'USERS_V1'
       ORDER BY ORDINAL_POSITION`
    );
    const actualCols = colRows.map((r) => r.COLUMN_NAME.toLowerCase());
    expect(actualCols).toEqual(["id", "name", "age"]);
  });
});
