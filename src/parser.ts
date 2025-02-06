import * as snowflake from "snowflake-sdk";
import * as fs from "fs";
import * as path from "path";
import csv from "csv-parser";
import { config } from "./config";
import { Metric } from "./types";

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
  } else {
    console.log("Successfully connected to Snowflake.");
  }
});
console.log("⏳ Parsing CSV file...");
const csvFilePath: string = path.join(__dirname, "data.csv");
const users = new Set<string>();
const metrics: Metric[] = [];
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
      profiles_viewed: Number(row["Profiles viewed"]),
      profiles_saved_to_project: Number(row["Profiles saved to project"]),
      inmails_sent: Number(row["InMails sent"]),
      inmails_accepted: Number(row["InMails accepted"]),
      inmails_declined: Number(row["InMails declined"]),
      start_date: row["Start date"],
      end_date: row["End date"],
    };
    metrics.push(metric);
  })
  .on("end", async () => {
    console.log("✅ CSV parsing completed.");
    await createSnowflakeTables();
    await insertDataToSnowflake();
    console.log("✅  Data upload to Snowflake completed.");
    connection.destroy((err) => {
      if (err) {
        console.error("Error disconnecting:", err.message);
      } else {
        console.log("Connection closed.");
      }
    });
  });

const createSnowflakeTables = async (): Promise<void> => {
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
      FOREIGN KEY (seat_id) REFERENCES users(id)
    );
  `;

  await executeQuery(userTableQuery);
  await executeQuery(metricsTableQuery);
  console.log("✅ tables created in Snowflake");
};

const insertDataToSnowflake = async (): Promise<void> => {
  console.log("⏳ Inserting data into Snowflake...");

  const insertUsersQuery = `
    MERGE INTO users AS target
    USING (VALUES ${[...users]
      .map((user) => {
        const parsedUser = JSON.parse(user);
        return `('${parsedUser.id}', '${parsedUser.name.replace(/'/g, "''")}')`;
      })
      .join(", ")}) AS source (id, name)
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET target.name = source.name
    WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);
  `;

  const insertMetricsQuery = `
    INSERT INTO metrics (seat_id, profiles_viewed, profiles_saved_to_project, inmails_sent, inmails_accepted, inmails_declined, start_date, end_date) VALUES ${metrics
      .map(
        (metric) => `(
          '${metric.seat_id}',
          '${metric.profiles_viewed}',
          '${metric.profiles_saved_to_project}',
          '${metric.inmails_sent}',
          '${metric.inmails_accepted}',
          '${metric.inmails_declined}',
          '${metric.start_date}',
          '${metric.end_date}'
        )`
      )
      .join(", ")};
  `;

  await executeQuery(insertUsersQuery);
  await executeQuery(insertMetricsQuery);
  console.log("✅ Data inserted into Snowflake");
};

function executeQuery(query: string): Promise<any> {
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
