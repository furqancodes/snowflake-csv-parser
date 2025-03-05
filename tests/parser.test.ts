import fs from "fs";
import path from "path";
import * as snowflake from "snowflake-sdk";
import { parser } from "../src/parser";
import { config } from "../src/config";
import { runQuery } from "../src/helpers/helper";

let connection: snowflake.Connection;

describe("parser integration tests (test DB)", () => {
  jest.setTimeout(100000);
  const TEST_DB = "TEST_DB";
  const TEST_SCHEMA = "TEST_SCHEMA";

  beforeAll(async () => {
    connection = snowflake.createConnection({
      account: config.snowflakeAccount,
      username: config.snowflakeUsername,
      password: config.snowflakePassword,
      database: TEST_DB,
      schema: TEST_SCHEMA,
    });

    await new Promise((resolve, reject) => {
      connection.connect((err) => (err ? reject(err) : resolve(null)));
    });
    await runQuery(connection, `DROP DATABASE IF EXISTS ${TEST_DB}`);
    await runQuery(connection, `CREATE DATABASE IF NOT EXISTS ${TEST_DB}`);
    await runQuery(connection, `CREATE SCHEMA IF NOT EXISTS ${TEST_SCHEMA}`);
    await runQuery(connection, `USE DATABASE ${TEST_DB}`);
    await runQuery(connection, `USE SCHEMA ${TEST_SCHEMA}`);

    config.snowflakeDatabase = TEST_DB;
    config.snowflakeSchema = TEST_SCHEMA;
  });

  afterEach(async () => {
    await runQuery(connection, `DROP TABLE users`);
    await runQuery(connection, `DROP TABLE metrics`);
  });

  afterAll(async () => {
    await runQuery(connection, `DROP DATABASE IF EXISTS ${TEST_DB}`);
    if (connection) {
      await new Promise((resolve) => {
        connection.destroy(() => {
          console.log("✅ Snowflake connection closed.");
          resolve(null);
        });
      });
    }
    const dataCsvPath = path.join(__dirname, "..", "src", "data.csv");
    if (fs.existsSync(dataCsvPath)) {
      fs.unlinkSync(dataCsvPath);
    }
  });

  function copyFixture(fixtureName: string) {
    const fixturePath = path.join(__dirname, "__fixtures__", fixtureName);
    const targetPath = path.join(__dirname, "..", "src", "data.csv");
    fs.copyFileSync(fixturePath, targetPath);
  }

  it("1) normal CSV inserts rows with no duplicates", async () => {
    copyFixture("normal.csv");
    await parser();
    const users = await runQuery(connection, "SELECT * FROM users;");
    expect(users.length).toBe(2);
    const metrics = await runQuery(connection, "SELECT * FROM metrics;");
    expect(metrics.length).toBe(2);
  });

  it("2) same seat_id, same date => only one metrics row", async () => {
    copyFixture("duplicateSameDate.csv");
    await parser();
    const users = await runQuery(connection, "SELECT * FROM users;");
    expect(users.length).toBe(1);
    const metrics = await runQuery(connection, "SELECT * FROM metrics;");
    expect(metrics.length).toBe(1);
  });

  it("3) same seat_id, different dates => multiple metrics rows", async () => {
    copyFixture("duplicateDifferentDate.csv");
    await parser();
    const users = await runQuery(connection, "SELECT * FROM users;");
    expect(users.length).toBe(1);
    const metrics = await runQuery(connection, "SELECT * FROM metrics;");
    expect(metrics.length).toBe(2);
  });

  it("4) type conversions properly inserted", async () => {
    copyFixture("typeConversion.csv");
    await parser();
    const rows = await runQuery(connection, "SELECT * FROM metrics;");
    expect(rows.length).toBeGreaterThan(0);
    expect(rows[0].PROFILES_VIEWED).toBe(100);
  });
});
