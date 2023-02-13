package org.learn.iceberg.debezium;

//import com.alibaba.ververica.cdc.debezium.internal.DebeziumOffset;
//import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

//import static com.alibaba.ververica.cdc.debezium.internal.DebeziumChangeConsumer.LAST_COMMIT_LSN_KEY;
//import static com.alibaba.ververica.cdc.debezium.internal.DebeziumChangeConsumer.LAST_COMPLETELY_PROCESSED_LSN_KEY;

public class DebeziumChangeConsumer<T>
    implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumChangeConsumer.class);
//  private final DebeziumOffset debeziumOffset;
  private DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> currentCommitter;

//  public DebeziumChangeConsumer() {
//    this.debeziumOffset = new DebeziumOffset();
//  }

  @Override
  public void handleBatch(
      List<ChangeEvent<SourceRecord, SourceRecord>> changeEvents,
      DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> recordCommitter) {
    this.currentCommitter = recordCommitter;
    try {
      for (ChangeEvent<SourceRecord, SourceRecord> event : changeEvents) {
        SourceRecord record = event.value();
        LOG.info(record.toString());
//        this.debeziumOffset.setSourcePartition(record.sourcePartition());
//        this.debeziumOffset.setSourceOffset(record.sourceOffset());

        //TODO
        //commitOffset();
      }
    } catch (Exception e) {
      LOG.error("Error happens when consuming change messages.", e);
    }
  }

  /**
   * We have to adjust type of LSN values to Long, because it might be Integer after
   * deserialization, however {@code
   * io.debezium.connector.postgresql.PostgresStreamingChangeEventSource#commitOffset(java.util.Map)}
   * requires Long.
   */
//  private Map<String, Object> adjustSourceOffset(Map<String, Object> sourceOffset) {
//    if (sourceOffset.containsKey(LAST_COMPLETELY_PROCESSED_LSN_KEY)) {
//      String value = sourceOffset.get(LAST_COMPLETELY_PROCESSED_LSN_KEY).toString();
//      sourceOffset.put(LAST_COMPLETELY_PROCESSED_LSN_KEY, Long.parseLong(value));
//    }
//    if (sourceOffset.containsKey(LAST_COMMIT_LSN_KEY)) {
//      String value = sourceOffset.get(LAST_COMMIT_LSN_KEY).toString();
//      sourceOffset.put(LAST_COMMIT_LSN_KEY, Long.parseLong(value));
//    }
//    return sourceOffset;
//  }

  //called when notifyCheckpointComplete
//  public void commitOffset(DebeziumOffset offset) throws InterruptedException {
//    if (currentCommitter == null) {
//      LOG.info(
//          "commitOffset() called on Debezium ChangeConsumer which doesn't receive records yet.");
//      return;
//    }

    // only the offset is used
//    SourceRecord recordWrapper =
//        new SourceRecord(
//            offset.sourcePartition,
//            adjustSourceOffset((Map<String, Object>) offset.sourceOffset),
//            "DUMMY",
//            Schema.BOOLEAN_SCHEMA,
//            true);
//    EmbeddedEngineChangeEvent<SourceRecord, SourceRecord> changeEvent =
//        new EmbeddedEngineChangeEvent<>(null, recordWrapper, recordWrapper);
//    currentCommitter.markProcessed(changeEvent);
//    currentCommitter.markBatchFinished();
//  }
}
