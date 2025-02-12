import * as snowflake from "snowflake-sdk";
import * as fs from "fs";
import * as path from "path";
import csv from "csv-parser";
import { config } from "./config";
import { Metric } from "./types";

export const parser = async (): Promise<void> => {
  const connection = snowflake.createConnection({
    account: config.snowflakeAccount,
    username: config.snowflakeUsername,
    password: config.snowflakePassword,
    warehouse: config.snowflakeWarehouse,
    database: config.snowflakeDatabase,
    schema: config.snowflakeSchema,
  });

  connection.connect((err, conn) => {
    if (err) {
      console.error("Unable to connect: " + err.message);
      throw "Error while connecting";
    } else {
      console.log("Successfully connected to Snowflake.");
    }
  });
  console.log("⏳ Parsing CSV file...");
  const csvFilePath: string = path.join(__dirname, "data.csv");
  const users = new Set<string>();
  const metrics: Metric[] = [];

  await new Promise<void>((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on("data", (row: Record<string, string>) => {
        const user = {
          name: row["Seat holder name"],
          id: row["Seat id"],
        };
        users.add(JSON.stringify(user));

        const metric: Metric = {
          seat_id: row["Seat id"],
          profiles_viewed: Number(row["Profiles viewed"] || 0),
          profiles_saved_to_project: Number(
            row["Profiles saved to project"] || 0
          ),
          inmails_sent: Number(row["InMails sent"] || 0),
          inmails_accepted: Number(row["InMails accepted"] || 0),
          inmails_declined: Number(row["InMails declined"] || 0),
          start_date: row["Start date"],
          end_date: row["End date"],
        };
        metrics.push(metric);
      })
      .on("end", async () => {
        console.log("✅ CSV parsing completed.");
        await createSnowflakeTables(connection);
        await upsertUsers(connection, users);

        const deduplicated = deduplicateMetrics(metrics);
        await upsertMetrics(connection, deduplicated);

        console.log("✅ Data upload to Snowflake completed.");
        connection.destroy((err) => {
          if (err) {
            console.error("Error disconnecting:", err.message);
          } else {
            console.log("Connection closed.");
          }
        });
        resolve();
      })
      .on("error", reject);
  });
};

function deduplicateMetrics(metrics: Metric[]): Metric[] {
  const map = new Map<string, Metric>();
  for (const m of metrics) {
    const key = `${m.seat_id}||${m.start_date}||${m.end_date}`;
    if (!map.has(key)) {
      map.set(key, m);
    }
  }
  return Array.from(map.values());
}

async function createSnowflakeTables(
  connection: snowflake.Connection
): Promise<void> {
  console.log("⏳ Creating tables in Snowflake...");
  const userTableQuery = `
    CREATE OR REPLACE TABLE users (
      id STRING PRIMARY KEY,
      name STRING
    );
  `;

  const metricsTableQuery = `
    CREATE OR REPLACE TABLE metrics (
      seat_id STRING,
      profiles_viewed STRING,
      profiles_saved_to_project STRING,
      inmails_sent STRING,
      inmails_accepted STRING,
      inmails_declined STRING,
      start_date TIMESTAMP,
      end_date TIMESTAMP,
      FOREIGN KEY (seat_id) REFERENCES users(id),
    )
    CLUSTER BY (seat_id, start_date, end_date);
  `;
  await executeQuery(connection, userTableQuery);
  await executeQuery(connection, metricsTableQuery);
  console.log("✅ Tables created in Snowflake");
}

async function upsertUsers(
  connection: snowflake.Connection,
  users: Set<string>
): Promise<void> {
  console.log("⏳ Upserting users into Snowflake...");
  if (users.size === 0) {
    console.log("No users found to upsert.");
    return;
  }
  const valuesPart = [...users]
    .map((userStr) => {
      const user = JSON.parse(userStr);
      const safeName = user.name.replace(/'/g, "''");
      return `('${user.id}', '${safeName}')`;
    })
    .join(", ");
  const mergeUsersQuery = `
    MERGE INTO users AS target
    USING (VALUES ${valuesPart}) AS source (id, name)
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET target.name = source.name
    WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);
  `;
  await executeQuery(connection, mergeUsersQuery);
  console.log("✅ Users upserted.");
}

async function upsertMetrics(
  connection: snowflake.Connection,
  metrics: Metric[]
): Promise<void> {
  console.log("⏳ Upserting metrics into Snowflake...");
  if (metrics.length === 0) {
    console.log("No metrics found to upsert.");
    return;
  }
  const valuesPart = metrics
    .map((m) => {
      return `(
        '${m.seat_id}',
        '${m.profiles_viewed}',
        '${m.profiles_saved_to_project}',
        '${m.inmails_sent}',
        '${m.inmails_accepted}',
        '${m.inmails_declined}',
        '${m.start_date}',
        '${m.end_date}'
      )`;
    })
    .join(", ");
  const mergeMetricsQuery = `
    MERGE INTO metrics AS target
    USING (
      VALUES ${valuesPart}
    ) AS source (
      seat_id, profiles_viewed, profiles_saved_to_project, inmails_sent,
      inmails_accepted, inmails_declined, start_date, end_date
    )
    ON (
      target.seat_id = source.seat_id
      AND target.start_date = source.start_date
      AND target.end_date = source.end_date
    )
    WHEN NOT MATCHED THEN INSERT (
      seat_id,
      profiles_viewed,
      profiles_saved_to_project,
      inmails_sent,
      inmails_accepted,
      inmails_declined,
      start_date,
      end_date
    )
    VALUES (
      source.seat_id,
      source.profiles_viewed,
      source.profiles_saved_to_project,
      source.inmails_sent,
      source.inmails_accepted,
      source.inmails_declined,
      source.start_date,
      source.end_date
    );
  `;
  await executeQuery(connection, mergeMetricsQuery);
  console.log("✅ Metrics upserted.");
}

function executeQuery(
  connection: snowflake.Connection,
  query: string
): Promise<any> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: query,
      complete: (err, stmt, rows) => {
        if (err) {
          console.error("Error executing query:", err.message);
          reject(err);
        } else {
          console.log("Query executed successfully");
          resolve(rows);
        }
      },
    });
  });
}
