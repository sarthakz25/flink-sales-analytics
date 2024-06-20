package com.sarthakz25;

import deserializer.JSONValueDeserializationSchema;
import dto.SalesPerCategory;
import dto.SalesPerDay;
import dto.SalesPerMonth;
import dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Date;

public class DataStreamJob {
    private static final String jdbcUrl = "jdbc:postgresql://localhost:5433/postgres";
    private static final String username = "postgres";
    private static final String password = "sar@123";

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // load configuration from external file
//        Configuration config = GlobalConfiguration.loadConfiguration();

//        String jdbcUrl = config.getString("jdbc.url", null);
//        String username = config.getString("jdbc.username", null);
//        String password = config.getString("jdbc.password", null);

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

        // transactions table
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
        )).name("Create transactions sink");

        // sales per category table
        transactionDataStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_category (" +
                        "transaction_date Date, " +
                        "category VARCHAR(255), " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY(transaction_date, category)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                executionOptions,
                connectionOptions
        )).name("Create sales per category sink");

        // sales per day table
        transactionDataStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_day (" +
                        "transaction_date Date PRIMARY KEY, " +
                        "total_sales DOUBLE PRECISION" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                executionOptions,
                connectionOptions
        )).name("Create sales per day sink");

        // sales per month table
        transactionDataStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_month (" +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY(year, month)" +
                        ")",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

                },
                executionOptions,
                connectionOptions
        )).name("Create sales per month sink");

        // insert data
        transactionDataStream.addSink(JdbcSink.sink(
                "INSERT INTO transactions(customer_id, transaction_id, transaction_date, product_id, " +
                        "product_name, product_category, product_price, product_quantity, product_brand, " +
                        "currency, payment_method, total_amount) " +
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (transaction_id) DO UPDATE SET " +
                        "customer_id = EXCLUDED.customer_id, " +
                        "transaction_date = EXCLUDED.transaction_date, " +
                        "product_id = EXCLUDED.product_id, " +
                        "product_name = EXCLUDED.product_name, " +
                        "product_category = EXCLUDED.product_category, " +
                        "product_price = EXCLUDED.product_price, " +
                        "product_quantity = EXCLUDED.product_quantity, " +
                        "product_brand = EXCLUDED.product_brand, " +
                        "currency = EXCLUDED.currency, " +
                        "payment_method = EXCLUDED.payment_method, " +
                        "total_amount = EXCLUDED.total_amount " +
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
        )).name("Insert into transactions sink");

        transactionDataStream.map(
                        transaction -> {
                            Date transactionDate = new Date(System.currentTimeMillis());
                            String category = transaction.getProductCategory();
                            double totalSales = transaction.getTotalAmount();

                            return new SalesPerCategory(transactionDate, category, totalSales);
                        }
                ).keyBy(SalesPerCategory::getCategory)
                .reduce((salesPerCategory, t1) -> {
                    salesPerCategory.setTotalSales(salesPerCategory.getTotalSales() + t1.getTotalSales());
                    return salesPerCategory;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_category(transaction_date, category, total_sales) " +
                                "VALUES(?, ?, ?) " +
                                "ON CONFLICT (transaction_date, category) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales " +
                                "WHERE sales_per_category.category = EXCLUDED.category " +
                                "AND sales_per_category.transaction_date = EXCLUDED.transaction_date",
                        (JdbcStatementBuilder<SalesPerCategory>) (preparedStatement, salesPerCategory) -> {
                            preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
                            preparedStatement.setString(2, salesPerCategory.getCategory());
                            preparedStatement.setDouble(3, salesPerCategory.getTotalSales());
                        },
                        executionOptions,
                        connectionOptions
                )).name("Insert into sales per category sink");

        transactionDataStream.map(
                        transaction -> {
                            Date transactionDate = new Date(System.currentTimeMillis());
                            double totalSales = transaction.getTotalAmount();

                            return new SalesPerDay(transactionDate, totalSales);
                        }
                ).keyBy(SalesPerDay::getTransactionDate)
                .reduce((salesPerDay, t1) -> {
                    salesPerDay.setTotalSales(salesPerDay.getTotalSales() + t1.getTotalSales());
                    return salesPerDay;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_day(transaction_date, total_sales) " +
                                "VALUES(?, ?) " +
                                "ON CONFLICT (transaction_date) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales " +
                                "WHERE sales_per_day.transaction_date = EXCLUDED.transaction_date",
                        (JdbcStatementBuilder<SalesPerDay>) (preparedStatement, salesPerDay) -> {
                            preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
                            preparedStatement.setDouble(2, salesPerDay.getTotalSales());
                        },
                        executionOptions,
                        connectionOptions
                )).name("Insert into sales per day sink");

        transactionDataStream.map(
                        transaction -> {
                            Date transactionDate = new Date(System.currentTimeMillis());
                            int year = transactionDate.toLocalDate().getYear();
                            int month = transactionDate.toLocalDate().getMonth().getValue();
                            double totalSales = transaction.getTotalAmount();

                            return new SalesPerMonth(year, month, totalSales);
                        }
                ).keyBy(SalesPerMonth::getMonth)
                .reduce((salesPerMonth, t1) -> {
                    salesPerMonth.setTotalSales(salesPerMonth.getTotalSales() + t1.getTotalSales());
                    return salesPerMonth;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_month(year, month, total_sales) " +
                                "VALUES(?, ?, ?) " +
                                "ON CONFLICT (year, month) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales " +
                                "WHERE sales_per_month.year = EXCLUDED.year " +
                                "AND sales_per_month.month = EXCLUDED.month",
                        (JdbcStatementBuilder<SalesPerMonth>) (preparedStatement, salesPerMonth) -> {
                            preparedStatement.setInt(1, salesPerMonth.getYear());
                            preparedStatement.setInt(2, salesPerMonth.getMonth());
                            preparedStatement.setDouble(3, salesPerMonth.getTotalSales());
                        },
                        executionOptions,
                        connectionOptions
                )).name("Insert into sales per month sink");

        // Execute program, beginning computation.
        env.execute("Flink Realtime Streaming");
    }
}
