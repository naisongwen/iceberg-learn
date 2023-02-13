/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.learn.iceberg.common;

import java.util.List;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;

/**
 * A stream source that:
 * 1) emits the elements from elementsPerCheckpoint.get(0) without allowing checkpoints.
 * 2) then waits for the checkpoint to complete.
 * 3) emits the elements from elementsPerCheckpoint.get(1) without allowing checkpoints.
 * 4) then waits for the checkpoint to complete.
 * 5) ...
 *
 * <p>Util all the list from elementsPerCheckpoint are exhausted.
 */
public final class UnBoundedTestSource implements SourceFunction<Record> {
  private Schema schema;
  private volatile boolean running = true;

  /**
   * Emits all those elements in several checkpoints.
   */
  public UnBoundedTestSource(Schema schema) {
    this.schema = schema;
  }

  @Override
  public void run(SourceContext<Record> ctx) throws InterruptedException {
    while (running) {
      List<Record> expectedRecords = RandomGenericData.generate(schema, 100, 1991L);
      for (Record record : expectedRecords) {
        if (record.get(0) != null) {
          ctx.collect(record);
        }
      }
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  // @Override
  // public TypeInformation getProducedType() {
  //   TypeInformation<Row> typeInfo = new RowTypeInfo(
  //       SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());
  //   return typeInfo;
  // }
}
