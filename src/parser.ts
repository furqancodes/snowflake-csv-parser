import snowflake from "snowflake-sdk";
import path from "path";
import dotenv from "dotenv";

dotenv.config();

const connection: snowflake.Connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT || "",
  username: process.env.SNOWFLAKE_USERNAME || "",
  password: process.env.SNOWFLAKE_PASSWORD || "",
  warehouse: process.env.SNOWFLAKE_WAREHOUSE || "",
  database: process.env.SNOWFLAKE_DATABASE || "",
  schema: process.env.SNOWFLAKE_SCHEMA || "",
});

connection.connectAsync((err, conn) => {
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
    const stageName: string = process.env.SNOWFLAKE_STAGE || "my_stage";
    const tableName: string =
      process.env.SNOWFLAKE_TABLE || "NEXHEALTH_HIRING_DETAIL";

    await executeQuery(`CREATE OR REPLACE STAGE ${stageName}`);
    const putCommand: string = `PUT 'file://${csvFilePath}' @${stageName}`;
    await executeQuery(putCommand);
    console.log("File uploaded to stage.");

    const copyCommand: string = `COPY INTO ${tableName} FROM @${stageName} FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)`;
    await executeQuery(copyCommand);
    console.log("Data copied into table.");

    await executeQuery(`REMOVE @${stageName}`);
    console.log("Stage cleaned up.");
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
