/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.translation.spark.sink.write;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

public class SeaTunnelWriteBuilder<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements WriteBuilder {

    private final SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;
    private final SeaTunnelWrite<AggregatedCommitInfoT, CommitInfoT, StateT> write;

    public SeaTunnelWriteBuilder(
            SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink) {
        this.sink = sink;
        this.write = new SeaTunnelWrite<AggregatedCommitInfoT, CommitInfoT, StateT>(sink);
    }

    @Override
    public BatchWrite buildForBatch() {
        return this.write.toBatch();
    }

    @Override
    public StreamingWrite buildForStreaming() {
        return this.write.toStreaming();
    }
}
