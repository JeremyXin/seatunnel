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

class InMemoryAutoscalerStateStoreTest {

    @Test
    void storesLatestAndBoundedHistoryWithoutDuplicatingIdentity() {
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(2);
        ScalingRecommendation first = recommendation(1L, 0L, ScalingAction.NO_ACTION);
        ScalingRecommendation second = recommendation(1L, 1L, ScalingAction.SCALE_OUT);
        ScalingRecommendation third = recommendation(1L, 2L, ScalingAction.SCALE_IN_CANDIDATE);

        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED, store.publish(first));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.DUPLICATE, store.publish(first));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED, store.publish(second));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED, store.publish(third));

        AutoscalerView view = store.view(true, true);
        Assertions.assertEquals(third, view.getLatestRecommendation());
        Assertions.assertEquals(2, view.getHistory().size());
        Assertions.assertEquals(second, view.getHistory().get(0));
        Assertions.assertEquals(third, view.getHistory().get(1));
    }

    @Test
    void rejectsStalePublication() {
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(2);

        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED,
                store.publish(recommendation(2L, 0L, ScalingAction.NO_ACTION)));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.REJECTED,
                store.publish(recommendation(1L, 9L, ScalingAction.SCALE_OUT)));
    }

    @Test
    void coalescesWaitingEvaluationsAndRecordsFiringTransition() {
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10);
        ScalingRecommendation waiting =
                recommendation(
                        1L,
                        0L,
                        ScalingAction.SCALE_OUT,
                        StabilizationTracker.StabilizationState.WAITING);
        ScalingRecommendation firing =
                recommendation(
                        1L,
                        2L,
                        ScalingAction.SCALE_OUT,
                        StabilizationTracker.StabilizationState.FIRING);
        AutoscalerMetricsSnapshot latestSnapshot =
                AutoscalerMetricsSnapshot.builder().currentWorkers(4).build();

        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED, store.publish(waiting));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED, store.publish(firing));
        store.updateCurrentSnapshot(latestSnapshot);

        AutoscalerView view = store.view(true, true);
        Assertions.assertEquals(firing, view.getLatestRecommendation());
        Assertions.assertEquals(latestSnapshot, view.getCurrentSnapshot());
        Assertions.assertEquals(2, view.getHistory().size());
        Assertions.assertEquals(waiting, view.getHistory().get(0));
        Assertions.assertEquals(firing, view.getHistory().get(1));
        Assertions.assertEquals(0L, view.getRecommendationCounts().get(ScalingAction.NO_ACTION));
        Assertions.assertEquals(1L, view.getRecommendationCounts().get(ScalingAction.SCALE_OUT));
    }

    private ScalingRecommendation recommendation(
            long masterEpoch, long generation, ScalingAction action) {
        return recommendation(
                masterEpoch, generation, action, StabilizationTracker.StabilizationState.NORMAL);
    }

    private ScalingRecommendation recommendation(
            long masterEpoch,
            long generation,
            ScalingAction action,
            StabilizationTracker.StabilizationState stabilizationState) {
        return ScalingRecommendation.builder()
                .masterEpoch(masterEpoch)
                .generation(generation)
                .action(action)
                .stabilizationState(stabilizationState)
                .currentWorkers(3)
                .recommendedWorkers(3)
                .observedAtMillis(100L + generation)
                .validUntilMillis(200L + generation)
                .snapshot(AutoscalerMetricsSnapshot.builder().currentWorkers(3).build())
                .recommendationOnly(true)
                .build();
    }
}
