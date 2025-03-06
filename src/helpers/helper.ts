// dbUtils.ts
import { Connection } from "snowflake-sdk";

export async function runQuery(
  connection: Connection,
  sqlText: string,
  binds: any[] = []
): Promise<any[]> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) => {
        if (err) {
          console.error("❌ Query failed:", err.message);
          reject(err);
        } else {
          console.log("✅ Query executed successfully");
          resolve(rows || []);
        }
      },
    });
  });
}
export function normalizeType(snowflakeType: string): string {
  const type = snowflakeType.toUpperCase();

  // If the DB says TEXT or VARCHAR(...) or VARCHAR(16777216), treat it as STRING
  if (type.startsWith("VARCHAR") || type === "TEXT") {
    return "STRING";
  }

  // If the DB says NUMBER(38,0), treat it as INTEGER
  if (type.startsWith("NUMBER")) {
    // Possibly check scale/precision if you want
    return "INTEGER";
  }

  // If the DB says TIMESTAMP_NTZ(9), treat it as TIMESTAMP
  if (type.startsWith("TIMESTAMP")) {
    return "TIMESTAMP";
  }

  // Otherwise, leave as-is
  return type;
}
