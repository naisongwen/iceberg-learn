/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.learn.datalake.debezium;

import io.debezium.engine.format.SerializationFormat;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * A {@link SerializationFormat} defining the {@link SourceRecord} type from Kafka Connect API.
 */
public class Connect implements SerializationFormat<SourceRecord> {
}
