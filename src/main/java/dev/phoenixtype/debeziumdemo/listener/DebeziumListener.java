package dev.phoenixtype.debeziumdemo.listener;

import dev.phoenixtype.debeziumdemo.service.CustomerService;
import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.debezium.data.Envelope.FieldName.*;
import static io.debezium.data.Envelope.Operation;
import static java.util.stream.Collectors.toMap;

/**
 * This class is a listener component that receives and processes change events from a Debezium connector
 */

@Slf4j
@Component
public class DebeziumListener {

    private final Executor executor = Executors.newSingleThreadExecutor(); // This creates a single-threaded executor, which will be used to execute the Debezium engine asynchronously.
    private final CustomerService customerService; // This is an instance of the CustomerService class, which is a service used to handle customer-related operations.
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine; // This is an instance of the DebeziumEngine class, which is responsible for capturing and processing change events.

    // The constructor public DebeziumListener(Configuration customerConnectorConfiguration, CustomerService customerService) is the constructor of the class.
    // It takes two parameters: customerConnectorConfiguration of type Configuration, which represents the configuration for the Debezium connector, and customerService of type CustomerService, which is used to handle customer-related operations.
    public DebeziumListener(Configuration customerConnectorConfiguration, CustomerService customerService) {

        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
            .using(customerConnectorConfiguration.asProperties())
            .notifying(this::handleChangeEvent)
            .build();

        this.customerService = customerService;
    }

    // RecordChangeEvent<SourceRecord> represents a change event for a specific record in the source database.
    // It provides access to the record's key, value, and metadata, allowing you to extract and process the relevant information associated with the change event.
    private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent) {
        SourceRecord sourceRecord = sourceRecordRecordChangeEvent.record(); // This line retrieves the SourceRecord from the RecordChangeEvent. The SourceRecord represents the actual record that has undergone a change.

        log.info("Key = '" + sourceRecord.key() + "' value = '" + sourceRecord.value() + "'"); // This line logs the key and value of the SourceRecord. It uses the log instance to log an informational message containing the key and value.

        Struct sourceRecordChangeValue= (Struct) sourceRecord.value(); // This line casts the value of the SourceRecord to a Struct. A Struct represents a structured data object in Debezium, typically corresponding to a row in a database table

        if (sourceRecordChangeValue != null) { // This conditional statement checks if the sourceRecordChangeValue is not null. If it is null, it means that the change event does not contain any actual data (e.g., a delete operation with no "before" value).
            Operation operation = Operation.forCode((String) sourceRecordChangeValue.get(OPERATION)); // This line retrieves the operation code from the sourceRecordChangeValue using the OPERATION field. It maps the operation code to an Operation enum value, which represents the type of operation performed on the record (e.g., INSERT, UPDATE, DELETE, READ).

            if(operation != Operation.READ) { // This condition checks if the operation is not a "READ" operation. If it is not a read operation, it proceeds to handle the change event.
                String record = operation == Operation.DELETE ? BEFORE : AFTER; // Handling Update & Insert operations. This line sets the record variable based on the operation type. If the operation is a "DELETE," it sets record to "BEFORE," otherwise, it sets it to "AFTER." This determines which part of the change event (before or after the update) will be processed.

                Struct struct = (Struct) sourceRecordChangeValue.get(record); // This line retrieves the relevant part of the change event (either "before" or "after") based on the record value. It casts it to a Struct object.
                Map<String, Object> payload = struct.schema().fields().stream()
                    .map(Field::name)
                    .filter(fieldName -> struct.get(fieldName) != null)
                    .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
                    .collect(toMap(Pair::getKey, Pair::getValue));

                // This block of code above processes the fields within the struct object. It obtains the schema of the struct object, iterates over its fields, filters out fields with null values, and creates a map (payload) that contains field names as keys and corresponding values.

                this.customerService.replicateData(payload, operation); // This line calls the replicateData method of the customerService instance, passing the payload map and the operation as arguments. It delegates the replication of the data to the customerService.
                log.info("Updated Data: {} with Operation: {}", payload, operation.name()); // This line logs an informational message containing the replicated
            }
        }
    }

    @PostConstruct //  indicating that it should be executed after the bean is constructed and all dependencies are injected.
    // this.executor.execute(debeziumEngine); executes the debeziumEngine asynchronously using the executor.
    // The executor is a single-threaded executor created earlier in the class. This line starts the Debezium engine, which is responsible for capturing and processing change events.
    private void start() {
        this.executor.execute(debeziumEngine);
    }

    @PreDestroy // indicating that it should be executed before the bean is destroyed.
    private void stop() throws IOException {
        if (this.debeziumEngine != null) { // check that the debeziumEngine is not null (has been initialized) before closing it.
            this.debeziumEngine.close(); // If the condition is true, this.debeziumEngine.close() is called. This closes the debeziumEngine and releases any resources associated with it.
        }
    }

    /*
        In summary, @PostConstruct is used to start the Debezium engine by executing it asynchronously using the executor.
        @PreDestroy is used to gracefully stop the Debezium engine by closing it and releasing associated resources before the bean is destroyed.
        These annotations ensure proper initialization and shutdown of the Debezium engine when the corresponding bean is managed by the Spring container.
     */
}