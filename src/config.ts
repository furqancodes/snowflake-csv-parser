import * as dotenv from "dotenv";
import { Config } from "./types";

dotenv.config();

export const config: Config = {
  snowflakeAccount: process.env.SNOWFLAKE_ACCOUNT as string,
  snowflakeUsername: process.env.SNOWFLAKE_USERNAME as string,
  snowflakePassword: process.env.SNOWFLAKE_PASSWORD as string,
  snowflakeWarehouse: process.env.SNOWFLAKE_WAREHOUSE as string,
  snowflakeDatabase: process.env.SNOWFLAKE_DATABASE as string,
  snowflakeSchema: process.env.SNOWFLAKE_SCHEMA as string,
  snowflakeStage: process.env.SNOWFLAKE_STAGE as string,
};
