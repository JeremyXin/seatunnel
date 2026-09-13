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

package org.apache.seatunnel.engine.server.autoscale;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultAutoScalerTest {

    @Test
    void publishesScaleOutWhileWaitingForStabilizationThenClampsTarget() {
        AutoscalerRuntimeConfig config =
                AutoscalerRuntimeConfig.builder()
                        .scaleOutStabilizationSeconds(300)
                        .scaleStep(2)
                        .maxWorkers(4)
                        .build();
        FakeTimeSource timeSource = new FakeTimeSource(1_000L, 0L);
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10);
        DefaultAutoScaler autoscaler =
                new DefaultAutoScaler(
                        7L,
                        config,
                        () -> baseSnapshot().currentWorkers(3).cpu(MetricValue.valid(0.9d)).build(),
                        new HierarchicalAutoscalingPolicy(DefaultAutoScaler.policyConfig(config)),
                        new StabilizationTracker(300_000L, 600_000L),
                        store,
                        timeSource);

        autoscaler.evaluateOnce();
        Assertions.assertEquals(
                ScalingAction.SCALE_OUT,
                store.view(true, true).getLatestRecommendation().getAction());

        timeSource.monotonicMillis = 1_000L;
        autoscaler.evaluateOnce();
        Assertions.assertEquals(1, store.view(true, true).getHistory().size());

        timeSource.monotonicMillis = 300_000L;
        autoscaler.evaluateOnce();

        ScalingRecommendation recommendation = store.view(true, true).getLatestRecommendation();
        Assertions.assertEquals(ScalingAction.SCALE_OUT, recommendation.getAction());
        Assertions.assertEquals(4, recommendation.getRecommendedWorkers());
        Assertions.assertEquals(7L, recommendation.getMasterEpoch());
        Assertions.assertEquals(2L, recommendation.getGeneration());
    }

    @Test
    void resetClearsGenerationAndStabilization() {
        AutoscalerRuntimeConfig config =
                AutoscalerRuntimeConfig.builder().scaleOutStabilizationSeconds(1).build();
        FakeTimeSource timeSource = new FakeTimeSource(1_000L, 0L);
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10);
        DefaultAutoScaler autoscaler =
                new DefaultAutoScaler(
                        8L,
                        config,
                        () -> baseSnapshot().cpu(MetricValue.valid(0.9d)).build(),
                        new HierarchicalAutoscalingPolicy(DefaultAutoScaler.policyConfig(config)),
                        new StabilizationTracker(1_000L, 1_000L),
                        store,
                        timeSource);

        autoscaler.evaluateOnce();
        autoscaler.reset(9L);
        autoscaler.evaluateOnce();

        ScalingRecommendation recommendation = store.view(true, true).getLatestRecommendation();
        Assertions.assertEquals(9L, recommendation.getMasterEpoch());
        Assertions.assertEquals(0L, recommendation.getGeneration());
    }

    @Test
    void republishesPersistentFiringRecommendationAfterRepeatInterval() {
        AutoscalerRuntimeConfig config =
                AutoscalerRuntimeConfig.builder()
                        .scaleOutStabilizationSeconds(1)
                        .recommendationRepeatSeconds(60)
                        .build();
        FakeTimeSource timeSource = new FakeTimeSource(1_000L, 0L);
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10);
        DefaultAutoScaler autoscaler =
                new DefaultAutoScaler(
                        10L,
                        config,
                        () -> baseSnapshot().cpu(MetricValue.valid(0.9d)).build(),
                        new HierarchicalAutoscalingPolicy(DefaultAutoScaler.policyConfig(config)),
                        new StabilizationTracker(1_000L, 1_000L),
                        store,
                        timeSource);

        autoscaler.evaluateOnce();
        timeSource.monotonicMillis = 1_000L;
        autoscaler.evaluateOnce();
        Assertions.assertEquals(2, store.view(true, true).getHistory().size());

        timeSource.monotonicMillis = 60_999L;
        autoscaler.evaluateOnce();
        Assertions.assertEquals(2, store.view(true, true).getHistory().size());

        timeSource.monotonicMillis = 61_000L;
        autoscaler.evaluateOnce();
        Assertions.assertEquals(3, store.view(true, true).getHistory().size());
        Assertions.assertEquals(
                ScalingAction.SCALE_OUT,
                store.view(true, true).getLatestRecommendation().getAction());
    }

    @Test
    void doesNotRepublishFiringRecommendationBeforeRepeatIntervalWhenTargetWorkerCountChanges() {
        AutoscalerRuntimeConfig config =
                AutoscalerRuntimeConfig.builder()
                        .scaleOutStabilizationSeconds(1)
                        .recommendationRepeatSeconds(60)
                        .build();
        FakeTimeSource timeSource = new FakeTimeSource(1_000L, 0L);
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10);
        int[] currentWorkers = {3};
        DefaultAutoScaler autoscaler =
                new DefaultAutoScaler(
                        11L,
                        config,
                        () ->
                                baseSnapshot()
                                        .currentWorkers(currentWorkers[0])
                                        .cpu(MetricValue.valid(0.9d))
                                        .build(),
                        new HierarchicalAutoscalingPolicy(DefaultAutoScaler.policyConfig(config)),
                        new StabilizationTracker(1_000L, 1_000L),
                        store,
                        timeSource);

        autoscaler.evaluateOnce();
        timeSource.monotonicMillis = 1_000L;
        autoscaler.evaluateOnce();
        Assertions.assertEquals(
                4, store.view(true, true).getLatestRecommendation().getRecommendedWorkers());

        currentWorkers[0] = 4;
        timeSource.monotonicMillis = 2_000L;
        autoscaler.evaluateOnce();

        Assertions.assertEquals(2, store.view(true, true).getHistory().size());
        Assertions.assertEquals(
                4, store.view(true, true).getLatestRecommendation().getRecommendedWorkers());
        Assertions.assertEquals(4, store.view(true, true).getCurrentSnapshot().getCurrentWorkers());
    }

    private AutoscalerMetricsSnapshot.Builder baseSnapshot() {
        return AutoscalerMetricsSnapshot.builder()
                .evaluationTimeMillis(1_000L)
                .currentWorkers(3)
                .minWorkers(1)
                .maxWorkers(10)
                .dynamicSlot(false)
                .fixedSlotUtilization(MetricValue.valid(0.1d))
                .cpu(MetricValue.valid(0.1d))
                .jvmMemory(MetricValue.valid(0.1d))
                .scaleInMetricsValid(true);
    }

    private static final class FakeTimeSource implements AutoscalerTimeSource {
        private long millis;
        private long monotonicMillis;

        private FakeTimeSource(long millis, long monotonicMillis) {
            this.millis = millis;
            this.monotonicMillis = monotonicMillis;
        }

        @Override
        public long currentTimeMillis() {
            return millis;
        }

        @Override
        public long monotonicTimeMillis() {
            return monotonicMillis;
        }
    }
}
