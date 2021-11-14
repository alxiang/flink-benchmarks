/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.createKeyedStateBackend;
import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.getValueState;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.setupKeyCount;

// my imports
import org.apache.flink.contrib.streaming.state.RedpandaStateBackend;
import org.apache.flink.contrib.streaming.state.RedpandaKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RedpandaKeyedStateBackendBuilder;

import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.core.fs.CloseableRegistry;
import java.util.Collections;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.AbstractStateBackend;
import java.io.File;


/** Implementation for listValue state benchmark testing. */
public class ValueStateBenchmark extends StateBenchmarkBase {
    private ValueState<Long> valueState;

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + ValueStateBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }

    @Setup
    public void setUp() throws Exception {

        if(false){
            keyedStateBackend = createKeyedStateBackend(backendType);
            valueState =
                getValueState(keyedStateBackend, new ValueStateDescriptor<>("kvState", Long.class));
        }
        else{
            // https://github.com/apache/flink/blob/39354f3bbf9e6d2a8fe0a5676dab3e1695249760/flink-state-backends/flink-statebackend-rocksdb/src/test/java/org/apache/flink/contrib/streaming/state/benchmark/StateBackendBenchmarkUtils.java
            RedpandaStateBackend backend = new RedpandaStateBackend();

            File rootDir = prepareDirectory("benchmark", null);
            File recoveryBaseDir = prepareDirectory("localRecovery", rootDir);

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
            int numberOfKeyGroups = keyGroupRange.getNumberOfKeyGroups();
            MetricGroup metricGroup = new UnregisteredMetricsGroup();

            ExecutionConfig executionConfig = new ExecutionConfig();
            LocalRecoveryConfig localRecoveryConfig = new LocalRecoveryConfig(
                                    false,
                                    new LocalRecoveryDirectoryProviderImpl(
                                            recoveryBaseDir, new JobID(), new JobVertexID(), 0));

            RedpandaKeyedStateBackendBuilder<Long> builder =
                    new RedpandaKeyedStateBackendBuilder<>(
                            "Test",
                            Thread.currentThread().getContextClassLoader(),
                            null,
                            LongSerializer.INSTANCE,
                            numberOfKeyGroups,
                            keyGroupRange,
                            executionConfig,
                            localRecoveryConfig,
                            TtlTimeProvider.DEFAULT,
                            LatencyTrackingStateConfig.disabled(),
                            metricGroup,
                            Collections.emptyList(),
                            AbstractStateBackend.getCompressionDecorator(executionConfig),
                            new CloseableRegistry());

            RedpandaKeyedStateBackend<Long> keyedStateBackend_ = builder.build();
            keyedStateBackend = keyedStateBackend_;

            valueState =
                getValueState(keyedStateBackend, new ValueStateDescriptor<>("Word counter", Long.class));
        }

        //----------------
        
        for (int i = 0; i < setupKeyCount; ++i) {
            keyedStateBackend.setCurrentKey((long) i);
            valueState.update(random.nextLong());
        }
        keyIndex = new AtomicInteger();
    }

    @Benchmark
    public void valueUpdate(KeyValue keyValue) throws IOException {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        valueState.update(keyValue.value);
    }

    @Benchmark
    public void valueAdd(KeyValue keyValue) throws IOException {
        keyedStateBackend.setCurrentKey(keyValue.newKey);
        valueState.update(keyValue.value);
    }

    @Benchmark
    public Long valueGet(KeyValue keyValue) throws IOException {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        return valueState.value();
    }

    // from here (private method): org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.prepareDirectory; 
    static File prepareDirectory(String prefix, File parentDir) throws IOException {
        File target = File.createTempFile(prefix, "", parentDir);
        if (target.exists() && !target.delete()) {
            throw new IOException(
                    "Target dir {"
                            + target.getAbsolutePath()
                            + "} exists but failed to clean it up");
        } else if (!target.mkdirs()) {
            throw new IOException("Failed to create target directory: " + target.getAbsolutePath());
        }
        return target;
    }
}
