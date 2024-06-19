package com.sarthakz25;

import deserializer.JSONValueDeserializationSchema;
import dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {
    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "sales_transactions";

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("flink-sales")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        DataStream<Transaction> transactionDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka source");

        transactionDataStream.print();

        JdbcExecutionOptions executionOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        // create transactions table
        transactionDataStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transactions (" +
                        "customer_id VARCHAR(255), " +
                        "transaction_id VARCHAR(255) PRIMARY KEY, " +
                        "transaction_date TIMESTAMP, " +
                        "product_id VARCHAR(255), " +
                        "product_name VARCHAR(255), " +
                        "product_category VARCHAR(255), " +
                        "product_price DOUBLE PRECISION, " +
                        "product_quantity INTEGER, " +
                        "product_brand VARCHAR(255), " +
                        "currency VARCHAR(255), " +
                        "payment_method VARCHAR(255), " +
                        "total_amount DOUBLE PRECISION" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                executionOptions,
                connectionOptions
        ));

        // insert data
        transactionDataStream.addSink(JdbcSink.sink(
                "INSERT INTO transactions(customer_id, transaction_id, transaction_date, product_id, " +
                        "product_name, product_category, product_price, product_quantity, product_brand, " +
                        "currency, payment_method, total_amount) " +
                        "VALUE(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (transaction_id) DO UPDATE SET " +
                        "customer_id = EXCLUDED.customer_id, " +
                        "transaction_date = EXCLUDED.transaction_date, " +
                        "product_id = EXCLUDED.product_id, " +
                        "product_name = EXCLUDED.product_name, " +
                        "product_category = EXCLUDED.product_category, " +
                        "product_price = EXCLUDED.product_price, " +
                        "product_quantity = EXCLUDED.product_quantity, " +
                        "product_brand = EXCLUDED.product_brand, " +
                        "total_amount = EXCLUDED.total_amount, " +
                        "currency = EXCLUDED.currency, " +
                        "payment_method = EXCLUDED.payment_method " +
                        "WHERE transactions.transaction_id = EXCLUDED.transaction_id",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
                    preparedStatement.setString(1, transaction.getCustomerId());
                    preparedStatement.setString(2, transaction.getTransactionId());
                    preparedStatement.setTimestamp(3, transaction.getTransactionDate());
                    preparedStatement.setString(4, transaction.getProductId());
                    preparedStatement.setString(5, transaction.getProductName());
                    preparedStatement.setString(6, transaction.getProductCategory());
                    preparedStatement.setDouble(7, transaction.getProductPrice());
                    preparedStatement.setInt(8, transaction.getProductQuantity());
                    preparedStatement.setString(9, transaction.getProductBrand());
                    preparedStatement.setString(10, transaction.getCurrency());
                    preparedStatement.setString(11, transaction.getPaymentMethod());
                    preparedStatement.setDouble(12, transaction.getTotalAmount());
                },
                executionOptions,
                connectionOptions
        )).name("Insert into transactions table sink");


        // Execute program, beginning computation.
        env.execute("Flink Realtime Streaming");
    }
}
