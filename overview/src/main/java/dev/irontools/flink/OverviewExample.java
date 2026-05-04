package dev.irontools.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class OverviewExample {
  public static void main(String[] args) {
    
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    settings.getConfiguration().setString("parallelism.default", "1");
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql("""
      CREATE TABLE Orders (
        order_number BIGINT,
        price        DECIMAL(10,2),
        buyer_id     STRING,
        category_id  INT,
        order_time   TIMESTAMP(3)
      ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '100',
        'fields.category_id.min' = '1',
        'fields.category_id.max' = '10'
      )
    """);

    tEnv.executeSql("SELECT * FROM Orders").print();

    tEnv.executeSql("""
      SELECT category_id,
             AVG(price) AS avg_price
      FROM Orders
      GROUP BY category_id
    """).print();

     tEnv.executeSql("""
       SELECT category_id,
              ROUND(AVG(price), 2) AS avg_price
       FROM Orders
       GROUP BY category_id
     """).print();
  }
}
