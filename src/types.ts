export type Metric = {
  seat_id: string;
  profiles_viewed: string | number;
  profiles_saved_to_project: string | number;
  inmails_sent: string | number;
  inmails_accepted: string | number;
  inmails_declined: string | number;
  start_date: string;
  end_date: string;
};
export interface Config {
  snowflakeAccount: string;
  snowflakeUsername: string;
  snowflakePassword: string;
  snowflakeWarehouse: string;
  snowflakeDatabase: string;
  snowflakeSchema: string;
  snowflakeStage: string;
}
