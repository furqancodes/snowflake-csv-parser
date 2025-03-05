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
