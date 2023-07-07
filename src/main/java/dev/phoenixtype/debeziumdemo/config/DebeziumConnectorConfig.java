package dev.phoenixtype.debeziumdemo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import java.io.File;
import java.io.IOException;

@Configuration
public class DebeziumConnectorConfig {

    /**
     * Database details.
     */
    @Value("${customer.datasource.host}")
    private String customerDbHost;

    @Value("${customer.datasource.database}")
    private String customerDbName;

    @Value("${customer.datasource.port}")
    private String customerDbPort;

    @Value("${customer.datasource.username}")
    private String customerDbUsername;

    @Value("${customer.datasource.password}")
    private String customerDbPassword;

    /**
     * Customer Database Connector Configuration
     * <br>
     *
     * <br>
     * This method creates a configuration object for the Debezium connector, which is used for capturing and streaming database changes. The method sets various configuration options such as the connector class, offset storage, database connection details, and history settings. The temporary files created are used for storing offset information and database history.
     */
    @Bean
    public io.debezium.config.Configuration customerConnector() throws IOException {
        File offsetStorageTempFile = File.createTempFile("offsets_", ".dat"); // This line creates a temporary file with a prefix "offsets_" and a suffix ".dat". The offsetStorageTempFile variable references this temporary file.
        File dbHistoryTempFile = File.createTempFile("dbhistory_", ".dat"); // This line creates a temporary file with a prefix "dbhistory_" and a suffix ".dat". The dbHistoryTempFile variable references this temporary file.
        return io.debezium.config.Configuration.create()
            .with("name", "customer-mysql-connector") // This line sets the name of the connector to "customer-mysql-connector".
            .with("connector.class", "io.debezium.connector.mysql.MySqlConnector") // This line sets the class of the connector to "io.debezium.connector.mysql.MySqlConnector".
            .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore") // This line sets the offset storage implementation to "org.apache.kafka.connect.storage.FileOffsetBackingStore".
            .with("offset.storage.file.filename", offsetStorageTempFile.getAbsolutePath()) // This line sets the filename of the offset storage file to the absolute path of the offsetStorageTempFile.
            .with("offset.flush.interval.ms", "60000") // This line sets the interval in milliseconds for flushing offsets to storage to 60,000 milliseconds (1 minute).
            .with("database.hostname", customerDbHost) // This line sets the hostname of the MySQL database server to the value of the customerDbHost variable.
            .with("database.port", customerDbPort) // This line sets the port of the MySQL database server to the value of the customerDbPort variable.
            .with("database.user", customerDbUsername) // This line sets the username for connecting to the customer's database.
            .with("database.password", customerDbPassword) // This line sets the password for connecting to the customer's database.
            .with("database.dbname", customerDbName) // This line sets the name of the customer's database.
            .with("database.include.list", customerDbName) // This line sets the list of databases or schemas to be monitored to the value of the customerDbName variable. This is a comma-separated list of database names.
            .with("include.schema.changes", "false") // This line specifies whether to include schema changes. Here, it is set to "false".
            .with("database.allowPublicKeyRetrieval", "true") // This line specifies whether to allow public key retrieval. Here, it is set to "true".
            .with("database.server.id", "10181") // This line sets the server ID for the database connector to 10181.
            .with("database.server.name", "customer-mysql-db-server") // This line sets the logical name of the database server to "customer-mysql-db-server".
            .with("database.history", "io.debezium.relational.history.FileDatabaseHistory") // This line sets the history implementation that should be used to store and recover database schema changes to "io.debezium.relational.history.FileDatabaseHistory".
            .with("database.history.file.filename", dbHistoryTempFile.getAbsolutePath()) // This line sets the filename of the database history file to the absolute path of the dbHistoryTempFile.
//          .with("topic.prefix", "my_topic_prefix") // Add the 'topic.prefix' property with a valid value
            .build();
    }
}

