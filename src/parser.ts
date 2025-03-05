import * as snowflake from "snowflake-sdk";
import * as fs from "fs";
import * as path from "path";
import csv from "csv-parser";
import { config } from "./config";
import { ColumnDefinition, Metric } from "./types";
import { getOrCreateVersionedTable } from "./utils/versionedTableUtils";
import { runQuery } from "./helpers/helper";

export const parser = async (): Promise<void> => {
  const connection = snowflake.createConnection({
    account: config.snowflakeAccount,
    username: config.snowflakeUsername,
    password: config.snowflakePassword,
    warehouse: config.snowflakeWarehouse,
    database: config.snowflakeDatabase,
    schema: config.snowflakeSchema,
  });

  // Connect (asynchronously)
  await new Promise<void>((resolve, reject) => {
    connection.connect((err) => {
      if (err) {
        console.error("❌ Unable to connect:", err.message);
        reject(new Error("Error while connecting to Snowflake"));
      } else {
        console.log("✅ Successfully connected to Snowflake.");
        resolve();
      }
    });
  });

  // 1) Decide desired columns for 'users' and 'metrics'
  const USER_COLUMNS: ColumnDefinition[] = [
    { name: "id", type: "STRING PRIMARY KEY" },
    { name: "name", type: "STRING" },
  ];
  const METRIC_COLUMNS: ColumnDefinition[] = [
    { name: "seat_id", type: "STRING" },
    { name: "profiles_viewed", type: "INTEGER" },
    { name: "profiles_saved_to_project", type: "INTEGER" },
    { name: "inmails_sent", type: "INTEGER" },
    { name: "inmails_accepted", type: "INTEGER" },
    { name: "inmails_declined", type: "INTEGER" },
    { name: "start_date", type: "TIMESTAMP" },
    { name: "end_date", type: "TIMESTAMP" },
  ];

  // 2) Get or create versioned tables
  const usersTableName = await getOrCreateVersionedTable(
    connection,
    config.snowflakeSchema,
    "users",
    USER_COLUMNS
  );
  const metricsTableName = await getOrCreateVersionedTable(
    connection,
    config.snowflakeSchema,
    "metrics",
    METRIC_COLUMNS
  );

  console.log("⏳ Parsing CSV file...");
  const csvFilePath: string = path.join(__dirname, "data.csv");
  const users = new Set<string>();
  const metrics: Metric[] = [];

  await new Promise<void>((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on("data", (row: Record<string, string>) => {
        const parseNumber = (value: string) =>
          Number(value.replace(/[\",]/g, "")) || 0;

        users.add(
          JSON.stringify({ id: row["Seat id"], name: row["Seat holder name"] })
        );

        metrics.push({
          seat_id: row["Seat id"],
          profiles_viewed: parseNumber(row["Profiles viewed"]),
          profiles_saved_to_project: parseNumber(
            row["Profiles saved to project"]
          ),
          inmails_sent: parseNumber(row["InMails sent"]),
          inmails_accepted: parseNumber(row["InMails accepted"]),
          inmails_declined: parseNumber(row["InMails declined"]),
          start_date: (row["Start date"] || "").split(" ")[0],
          end_date: (row["End date"] || "").split(" ")[0],
        });
      })
      .on("end", async () => {
        console.log("✅ CSV parsing completed.");

        // 3) Upsert data into your versioned tables
        await upsertUsers(connection, usersTableName, users);
        await upsertMetrics(
          connection,
          metricsTableName,
          deduplicateMetrics(metrics)
        );

        console.log("✅ Data upload to Snowflake completed.");

        // 4) Clean up connection
        await new Promise((resolve) => {
          connection.destroy((err) => {
            if (err) {
              console.error("❌ Error disconnecting:", err.message);
            } else {
              console.log("✅ Snowflake connection closed.");
            }
            resolve(null);
          });
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

async function upsertUsers(
  connection: snowflake.Connection,
  tableName: string,
  users: Set<string>
): Promise<void> {
  console.log(`⏳ Upserting users into ${tableName} ...`);
  if (users.size === 0) {
    console.log("No users found to upsert.");
    return;
  }

  // Build a VALUES list for the MERGE
  const valuesPart = [...users]
    .map((userStr) => {
      const user = JSON.parse(userStr);
      const safeName = user.name.replace(/'/g, "''");
      return `('${user.id}', '${safeName}')`;
    })
    .join(", ");

  const mergeUsersQuery = `
    MERGE INTO ${tableName} AS target
    USING (VALUES ${valuesPart}) AS source (id, name)
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET target.name = source.name
    WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);
  `;
  await runQuery(connection, mergeUsersQuery);
  console.log("✅ Users upserted.");
}

async function upsertMetrics(
  connection: snowflake.Connection,
  tableName: string,
  metrics: Metric[]
): Promise<void> {
  console.log(`⏳ Upserting metrics into ${tableName} ...`);
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
    MERGE INTO ${tableName} AS target
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
  await runQuery(connection, mergeMetricsQuery);
  console.log("✅ Metrics upserted.");
}
