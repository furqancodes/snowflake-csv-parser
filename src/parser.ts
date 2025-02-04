import * as snowflake from "snowflake-sdk";
import * as fs from "fs";
import * as path from "path";
import dotenv from "dotenv";
import { config } from "./config";

dotenv.config();

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
    uploadCsv();
  }
});

async function uploadCsv(): Promise<void> {
  try {
    const csvFilePath: string = path.join(__dirname, "data.csv");
    const stageName: string = config.snowflakeStage || "my_stage";

    console.log("🚀 Starting upload process...");

    console.log("⏳ Creating Snowflake stage...");
    await executeQuery(
      `CREATE OR REPLACE STAGE ${config.snowflakeSchema}.${stageName};`
    );
    console.log("✅ Stage created successfully.");

    console.log("⏳ Uploading CSV to Snowflake stage...");
    await executeQuery(
      `PUT file://${csvFilePath} @${config.snowflakeSchema}.${stageName} AUTO_COMPRESS=TRUE;`
    );
    console.log("✅ File uploaded successfully.");

    console.log("⏳ Loading data into temp table...");
    await executeQuery(`
      CREATE OR REPLACE TEMP TABLE ${config.snowflakeSchema}.temp_metrics (
        "Seat holder name" STRING,
        "Seat id" NUMBER,
        "Profiles viewed" STRING,
        "Projects created" STRING,
        "Profiles saved to project" STRING,
        "Searches saved" STRING,
        "Searches performed" STRING,
        "InMails sent" STRING,
        "InMails accepted" STRING,
        "InMails declined" STRING,
        "Start date" DATE,
        "End date" DATE,
        "Created At" TIMESTAMP
      );
    `);

    await executeQuery(`
      COPY INTO ${config.snowflakeSchema}.temp_metrics
      FROM @${config.snowflakeSchema}.${config.snowflakeStage}/data.csv
      FILE_FORMAT = (
        TYPE = CSV,
        SKIP_HEADER = 1,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
      );
    `);
    console.log("✅ Data loaded into temp table.");

    console.log("⏳ Inserting unique users into users table...");
    await executeQuery(`
      INSERT INTO ${config.snowflakeSchema}.users ("Seat id", "Seat holder name")
      SELECT DISTINCT "Seat id", "Seat holder name"
      FROM ${config.snowflakeSchema}.temp_metrics
      WHERE "Seat id" IS NOT NULL
      AND "Seat id" NOT IN (SELECT "Seat id" FROM ${config.snowflakeSchema}.users);
    `);
    console.log("✅ Users inserted successfully.");

    console.log("⏳ Inserting metrics data...");
    await executeQuery(`
      INSERT INTO ${config.snowflakeSchema}.metrics (
        "Seat id", "Profiles viewed", "Projects created", "Profiles saved to project",
        "Searches saved", "Searches performed", "InMails sent", "InMails accepted",
        "InMails declined", "Start date", "End date", "Created At"
      )
      SELECT
        t."Seat id",
        TRY_CAST(t."Profiles viewed" AS NUMBER),
        TRY_CAST(t."Projects created" AS NUMBER),
        TRY_CAST(t."Profiles saved to project" AS NUMBER),
        TRY_CAST(t."Searches saved" AS NUMBER),
        TRY_CAST(t."Searches performed" AS NUMBER),
        TRY_CAST(t."InMails sent" AS NUMBER),
        TRY_CAST(t."InMails accepted" AS NUMBER),
        TRY_CAST(t."InMails declined" AS NUMBER),
        t."Start date",
        t."End date",
        t."Created At"
      FROM ${config.snowflakeSchema}.temp_metrics t
      INNER JOIN ${config.snowflakeSchema}.users u ON t."Seat id" = u."Seat id";
    `);
    console.log("✅ Metrics inserted successfully.");
  } catch (error) {
    console.error("Error uploading CSV:", error);
  } finally {
    connection.destroy((err) => {
      if (err) {
        console.error("Error disconnecting:", err.message);
      } else {
        console.log("Connection closed.");
      }
    });
  }
}

function executeQuery(query: string): Promise<any> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: query,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      },
    });
  });
}
