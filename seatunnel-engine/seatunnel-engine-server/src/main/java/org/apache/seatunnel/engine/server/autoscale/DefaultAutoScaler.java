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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Runs one advisory autoscaler evaluation at a time for the active master.
 *
 * <p>It collects signals, applies policy and stabilization, then publishes a fenced
 * recommendation-only result.
 */
public final class DefaultAutoScaler {

    private final AutoscalerRuntimeConfig config;
    private final AutoscalerSignalCollector signalCollector;
    private final AutoscalingPolicy policy;
    private final StabilizationTracker stabilizationTracker;
    private final AutoscalerStateStore stateStore;
    private final AutoscalerTimeSource timeSource;

    private long masterEpoch;
    private long generation;
    private volatile boolean closed;
    private ScalingAction lastPublishedAction;
    private StabilizationTracker.StabilizationState lastPublishedState;
    private long lastPublishedTimeMillis = -1L;

    public DefaultAutoScaler(
            long masterEpoch,
            AutoscalerRuntimeConfig config,
            AutoscalerSignalCollector signalCollector,
            AutoscalingPolicy policy,
            StabilizationTracker stabilizationTracker,
            AutoscalerStateStore stateStore,
            AutoscalerTimeSource timeSource) {
        this.masterEpoch = masterEpoch;
        this.config = Objects.requireNonNull(config, "config");
        this.signalCollector = Objects.requireNonNull(signalCollector, "signalCollector");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.stabilizationTracker =
                Objects.requireNonNull(stabilizationTracker, "stabilizationTracker");
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    }

    public static AutoscalerPolicyConfig policyConfig(AutoscalerRuntimeConfig config) {
        return AutoscalerPolicyConfig.builder()
                .scaleOutCpuThreshold(config.getScaleOutCpuThreshold())
                .scaleOutJvmMemoryThreshold(config.getScaleOutJvmMemoryThreshold())
                .scaleInCpuThreshold(config.getScaleInCpuThreshold())
                .scaleInJvmMemoryThreshold(config.getScaleInJvmMemoryThreshold())
                .fixedSlotScaleOutThreshold(config.getFixedSlotScaleOutThreshold())
                .fixedSlotScaleInThreshold(config.getFixedSlotScaleInThreshold())
                .build();
    }

    public static StabilizationTracker stabilizationTracker(AutoscalerRuntimeConfig config) {
        return new StabilizationTracker(
                TimeUnit.SECONDS.toMillis(config.getScaleOutStabilizationSeconds()),
                TimeUnit.SECONDS.toMillis(config.getScaleInStabilizationSeconds()));
    }

    /**
     * Collects metrics, evaluates the policy, and publishes a stabilized scaling recommendation.
     */
    public synchronized RecommendationFence.PublicationResult evaluateOnce() {
        if (closed) {
            return RecommendationFence.PublicationResult.REJECTED;
        }
        AutoscalerMetricsSnapshot snapshot = signalCollector.collect();
        stateStore.updateCurrentSnapshot(snapshot);
        AutoscaleEvaluation evaluation = policy.evaluate(snapshot);
        StabilizationTracker.StabilizationState stabilizationState =
                stabilizationTracker.evaluate(
                        evaluation.getAction(), timeSource.monotonicTimeMillis());
        ScalingRecommendation recommendation =
                ScalingRecommendation.builder()
                        .masterEpoch(masterEpoch)
                        .generation(generation++)
                        .action(evaluation.getAction())
                        .stabilizationState(stabilizationState)
                        .currentWorkers(snapshot.getCurrentWorkers())
                        .recommendedWorkers(recommendedWorkers(evaluation.getAction(), snapshot))
                        .observedAtMillis(timeSource.currentTimeMillis())
                        .validUntilMillis(
                                timeSource.currentTimeMillis()
                                        + TimeUnit.SECONDS.toMillis(
                                                config.getEvaluationIntervalSeconds()))
                        .decisionReasons(evaluation.getDecisionReasons())
                        .snapshot(snapshot)
                        .recommendationOnly(true)
                        .build();
        long currentTimeMillis = timeSource.monotonicTimeMillis();
        // publish recommendation if state changed.
        boolean stateChanged =
                hasPublicationStateChanged(evaluation.getAction(), stabilizationState);
        // publish recommendation if the "firing" status persists for longer than the specified
        // configuration duration
        boolean repeatIntervalElapsed =
                isRepeatRecommendationDue(stabilizationState, currentTimeMillis);
        boolean shouldPublish = stateChanged || repeatIntervalElapsed;
        RecommendationFence.PublicationResult result =
                shouldPublish
                        ? stateStore.publish(recommendation)
                        : RecommendationFence.PublicationResult.ACCEPTED;
        if (shouldPublish && result == RecommendationFence.PublicationResult.ACCEPTED) {
            lastPublishedAction = evaluation.getAction();
            lastPublishedState = stabilizationState;
            lastPublishedTimeMillis = currentTimeMillis;
        }
        return result;
    }

    public synchronized void close() {
        closed = true;
    }

    public synchronized void reset(long masterEpoch) {
        this.masterEpoch = masterEpoch;
        this.generation = 0L;
        this.lastPublishedAction = null;
        this.lastPublishedState = null;
        this.lastPublishedTimeMillis = -1L;
        this.stabilizationTracker.reset();
    }

    public synchronized long getMasterEpoch() {
        return masterEpoch;
    }

    public synchronized long getNextGeneration() {
        return generation;
    }

    private boolean hasPublicationStateChanged(
            ScalingAction evaluatedAction,
            StabilizationTracker.StabilizationState stabilizationState) {
        return lastPublishedAction != evaluatedAction || lastPublishedState != stabilizationState;
    }

    private boolean isRepeatRecommendationDue(
            StabilizationTracker.StabilizationState stabilizationState, long currentTimeMillis) {
        return stabilizationState == StabilizationTracker.StabilizationState.FIRING
                && lastPublishedState == StabilizationTracker.StabilizationState.FIRING
                && currentTimeMillis - lastPublishedTimeMillis
                        >= TimeUnit.SECONDS.toMillis(config.getRecommendationRepeatSeconds());
    }

    private int recommendedWorkers(ScalingAction action, AutoscalerMetricsSnapshot snapshot) {
        int currentWorkers = snapshot.getCurrentWorkers();
        if (action == ScalingAction.SCALE_OUT) {
            return Math.min(config.getMaxWorkers(), currentWorkers + config.getScaleStep());
        }
        if (action == ScalingAction.SCALE_IN_CANDIDATE) {
            return Math.max(config.getMinWorkers(), currentWorkers - config.getScaleStep());
        }
        return currentWorkers;
    }
}
