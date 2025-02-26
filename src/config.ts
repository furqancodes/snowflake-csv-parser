import * as dotenv from "dotenv";
import { ConfigType } from "./types";

dotenv.config();

export const config: ConfigType = {
  snowflakeAccount: process.env.SNOWFLAKE_ACCOUNT as string,
  snowflakeUsername: process.env.SNOWFLAKE_USERNAME as string,
  snowflakePassword: process.env.SNOWFLAKE_PASSWORD as string,
  snowflakeWarehouse: process.env.SNOWFLAKE_WAREHOUSE as string,
  snowflakeDatabase: process.env.SNOWFLAKE_DATABASE as string,
  snowflakeSchema: process.env.SNOWFLAKE_SCHEMA as string,
};

// Validate that none of these are empty
const missingKeys = Object.entries(config)
  .filter(([_, value]) => !value)
  .map(([key]) => key);
if (missingKeys.length > 0) {
  throw new Error(
    `❌ Missing environment variables: ${missingKeys.join(", ")}`
  );
}
