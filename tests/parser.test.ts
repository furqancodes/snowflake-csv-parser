import fs from "fs";
import path from "path";
import * as snowflake from "snowflake-sdk";
import { parser } from "../src/parser";
import { config } from "../src/config";

function runSnowflakeQuery(sqlText: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const connection = snowflake.createConnection({
      account: config.snowflakeAccount,
      username: config.snowflakeUsername,
      password: config.snowflakePassword,
      database: "TEST_DB",
      schema: "TEST_SCHEMA",
    });
    connection.connect((err) => {
      if (err) return reject(err);
      connection.execute({
        sqlText,
        complete: (execErr, stmt, rows) => {
          connection.destroy(() => {
            if (execErr) reject(execErr);
            else resolve(rows || []);
          });
        },
      });
    });
  });
}

describe("parser integration tests (test DB)", () => {
  jest.setTimeout(30000);

  const TEST_DB = "TEST_DB";
  const TEST_SCHEMA = "TEST_SCHEMA";

  beforeAll(async () => {
    await runSnowflakeQuery(`CREATE DATABASE IF NOT EXISTS ${TEST_DB}`);
    await runSnowflakeQuery(`CREATE SCHEMA IF NOT EXISTS ${TEST_SCHEMA}`);
    await runSnowflakeQuery(`USE DATABASE ${TEST_DB}`);
    await runSnowflakeQuery(`USE SCHEMA ${TEST_SCHEMA}`);
    config.snowflakeDatabase = TEST_DB;
    config.snowflakeSchema = TEST_SCHEMA;
  });

  afterAll(async () => {
    const dataCsvPath = path.join(__dirname, "..", "src", "data.csv");
    if (fs.existsSync(dataCsvPath)) {
      fs.unlinkSync(dataCsvPath);
    }

    config.snowflakeDatabase = process.env.SNOWFLAKE_DATABASE || "";
    config.snowflakeSchema = process.env.SNOWFLAKE_SCHEMA || "";
  });

  function copyFixture(fixtureName: string) {
    const fixturePath = path.join(__dirname, "__fixtures__", fixtureName);
    const targetPath = path.join(__dirname, "..", "src", "data.csv");
    fs.copyFileSync(fixturePath, targetPath);
  }

  it("1) normal CSV inserts rows with no duplicates", async () => {
    copyFixture("normal.csv");
    await parser();
    const users = await runSnowflakeQuery("SELECT * FROM users;");
    expect(users.length).toBe(2);
    const metrics = await runSnowflakeQuery("SELECT * FROM metrics;");
    expect(metrics.length).toBe(2);
  });

  it("2) same seat_id, same date => only one metrics row", async () => {
    copyFixture("duplicateSameDate.csv");
    await parser();
    const users = await runSnowflakeQuery("SELECT * FROM users;");
    expect(users.length).toBe(1);
    const metrics = await runSnowflakeQuery("SELECT * FROM metrics;");
    expect(metrics.length).toBe(1);
  });

  it("3) same seat_id, different dates => multiple metrics rows", async () => {
    copyFixture("duplicateDifferentDate.csv");
    await parser();
    const users = await runSnowflakeQuery("SELECT * FROM users;");
    expect(users.length).toBe(1);
    const metrics = await runSnowflakeQuery("SELECT * FROM metrics;");
    expect(metrics.length).toBe(2);
  });

  it("4) type conversions properly inserted", async () => {
    copyFixture("typeConversion.csv");
    await parser();
    const rows = await runSnowflakeQuery("SELECT * FROM metrics;");
    expect(rows.length).toBeGreaterThan(0);
    expect(rows[0].PROFILES_VIEWED).toBe("100");
  });
});
