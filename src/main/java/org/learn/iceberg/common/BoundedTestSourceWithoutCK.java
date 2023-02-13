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

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

public final class BoundedTestSourceWithoutCK<T> implements SourceFunction<T> {

    private final List<T> elements;
    private volatile boolean running = true;


    /**
     * Emits all those elements in a single checkpoint.
     */
    public BoundedTestSourceWithoutCK(T... elements) {
        this.elements = Arrays.asList(elements);
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        for (T element : elements) {
            ctx.collect(element);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
