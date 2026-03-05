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
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.taskmanager;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.io.network.partition.OriginalTaskDataFetcher;
import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;
import com.huawei.omniruntime.flink.runtime.metrics.groups.OmniTaskMetricGroup;
import com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper;
import com.huawei.omniruntime.flink.runtime.state.TaskStateManagerWrapper;
import com.huawei.omniruntime.flink.runtime.taskexecutor.TaskOperatorGatewayWrapper;
import com.huawei.omniruntime.flink.streaming.api.graph.JobType;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBStateUploader;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.BufferWritingResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.ChannelStatePersister;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.OmniLocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.OmniRemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.OperatorEventDispatcherImpl;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * customized OmniTask for OmniRuntime
 *
 * @since 2025-02-16
 */
public class OmniTask extends Task {
    private static final Logger LOG = LoggerFactory.getLogger(OmniTask.class);
    private static final String OMNI_SOURCE_STREAM_TASK_CLASS_NAME =
            "com.huawei.omniruntime.flink.runtime.tasks.OmniSourceStreamTask";
    private static final String SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME =
            "org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask";
    private static final String SOURCE_STREAM_TASK_CLASS_NAME =
            "org.apache.flink.streaming.runtime.tasks.SourceStreamTask";
    private static final String OMNI_SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME =
            "com.huawei.omniruntime.flink.runtime.tasks.OmniSourceOperatorStreamTask";
    private static final String OMNI_ONEINPUT_STREAM_TASK_CLASS_NAME =
            "com.huawei.omniruntime.flink.runtime.tasks.OmniOneInputStreamTaskV2";
    private static final String OMNI_TWOINPUT_STREAM_TASK_CLASS_NAME =
            "com.huawei.omniruntime.flink.runtime.tasks.OmniTwoInputStreamTaskV2";
    private static final String ONEINPUT_STREAM_TASK_CLASS_NAME =
            "org.apache.flink.streaming.runtime.tasks.OneInputStreamTask";
    private static final String TWOINPUT_STREAM_TASK_CLASS_NAME =
            "org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask";

    private long nativeTaskRef = 0L;
    private JobType jobType; // 0 default vanilla java task, 1 native sql task, 2 native datastream task. 3 future - hybrid java+cpp source task

    // dup fields of the Task as those field are prviate in parent class Task
    private JobInformation __jobInformation;
    private TaskInformation __taskInformation;
    private OriginalTaskDataFetcher originalTaskDataFetcher;
    private OperatorEventDispatcherImpl operatorEventDispatcher;
    private Map<OperatorID, OperatorEventHandler> operatorEventHandlers;

    /**
     * The name of the alias class for omnistream
     */
    private String aliasOfInvokableClass;
    private long nativeTaskMetricGroupRef;
    private OmniTaskMetricGroup omniTaskMetricGroup;
    private Map<ExecutionAttemptID,OmniTaskReferenceCounter> taskSlotTable;

    /**
     * checkpointing
     */
    private TaskStateManagerWrapper taskStateManagerWrapper;
    private TaskOperatorGatewayWrapper taskOperatorGatewayWrapper;

    private CheckpointOptions checkpointOptions;
    private CheckpointStreamFactory checkpointStreamFactory;

    // temporarily use public for easy access from OmniTaskWrapper
    public RuntimeEnvironment checkpointingEnv;
    
    /**
     * <b>IMPORTANT:</b> This constructor may not start any work that would need to be undone in the
     * case of a failing task deployment.
     *
     * @param jobInformation
     * @param taskInformation
     * @param executionAttemptID
     * @param slotAllocationId
     * @param resultPartitionDeploymentDescriptors
     * @param inputGateDeploymentDescriptors
     * @param memManager
     * @param ioManager
     * @param shuffleEnvironment
     * @param kvStateService
     * @param bcVarManager
     * @param taskEventDispatcher
     * @param externalResourceInfoProvider
     * @param taskStateManager
     * @param taskManagerActions
     * @param inputSplitProvider
     * @param checkpointResponder
     * @param operatorCoordinatorEventGateway
     * @param aggregateManager
     * @param classLoaderHandle
     * @param fileCache
     * @param taskManagerConfig
     * @param metricGroup
     * @param partitionProducerStateChecker
     * @param executor
     */
    public OmniTask(JobInformation jobInformation, TaskInformation taskInformation,
                    ExecutionAttemptID executionAttemptID, AllocationID slotAllocationId,
                    List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
                    List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors, MemoryManager memManager,
                    IOManager ioManager, ShuffleEnvironment<?, ?> shuffleEnvironment, KvStateService kvStateService,
                    BroadcastVariableManager bcVarManager, TaskEventDispatcher taskEventDispatcher,
                    ExternalResourceInfoProvider externalResourceInfoProvider, TaskStateManager taskStateManager,
                    TaskManagerActions taskManagerActions, InputSplitProvider inputSplitProvider,
                    CheckpointResponder checkpointResponder, TaskOperatorEventGateway operatorCoordinatorEventGateway,
                    GlobalAggregateManager aggregateManager, LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
                    FileCache fileCache, TaskManagerRuntimeInfo taskManagerConfig, @Nonnull TaskMetricGroup metricGroup,
                    PartitionProducerStateChecker partitionProducerStateChecker, Executor executor,Map<ExecutionAttemptID,OmniTaskReferenceCounter> taskSlotTable,
                    TaskStateManagerWrapper taskStateManagerWrapper, TaskOperatorGatewayWrapper taskOperatorGatewayWrapper) {
        super(jobInformation, taskInformation, executionAttemptID, slotAllocationId,
                resultPartitionDeploymentDescriptors, inputGateDeploymentDescriptors, memManager, ioManager,
                shuffleEnvironment, kvStateService, bcVarManager, taskEventDispatcher, externalResourceInfoProvider,
                taskStateManager, taskManagerActions, inputSplitProvider, checkpointResponder,
                operatorCoordinatorEventGateway, aggregateManager, classLoaderHandle, fileCache, taskManagerConfig,
                metricGroup, partitionProducerStateChecker, executor);
        this.__taskInformation = taskInformation;
        this.__jobInformation = jobInformation;
        this.jobType = JobType.NULL;
        this.taskSlotTable = taskSlotTable;
        this.taskStateManagerWrapper = taskStateManagerWrapper;
        this.taskOperatorGatewayWrapper=taskOperatorGatewayWrapper;
    }

    public TaskStateManagerWrapper getTaskStateManagerWrapper() {
        return taskStateManagerWrapper;
    }

    /**
     * The core work method that bootstraps the task and executes its code.
     */
    @Override
    public void run() {
        try {
            doRun();
        } finally {
            terminationFuture.complete(executionState);
        }
    }
    private void doRun() {
        // ----------------------------
        //  Initial State transition
        // ----------------------------
        initState();
        // all resource acquisitions and registrations from here on
        // need to be undone in the end
        Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
        TaskInvokable invokable = null;

        try {
            // ----------------------------
            //  Task Bootstrap - We periodically
            //  check for canceling as a shortcut
            // ----------------------------
            Environment env = prepareEnvironment(distributedCacheEntries);
            invokable = createInitAndInvokeTask(env);
        } catch (Throwable t) {
            // ----------------------------------------------------------------
            // the execution failed. either the invokable code properly failed, or
            // an exception was thrown as a side effect of cancelling
            // ----------------------------------------------------------------
            t = preProcessException(t);
            try {
                // transition into our final state. we should be either in DEPLOYING, INITIALIZING,
                // RUNNING, CANCELING, or FAILED
                // loop for multiple retries during concurrent state changes via calls to cancel()
                // or to failExternally()
                throwableHandle(t, invokable);
            } catch (Throwable tt) {
                String message = String.format("FATAL - exception in exception handler of task %s (%s).",
                        taskNameWithSubtask, executionId);
                LOG.error(message, tt);
                notifyFatalError(message, tt);
            }
        } finally {
            try {
                LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);
                if (originalTaskDataFetcher != null) {
                    originalTaskDataFetcher.finishRunning();
                }
                deleteParentTaskInSlotTable();
                // clear the reference to the invokable. this helps guard against holding references
                // to the invokable and its structures in cases where this Task object is still
                // referenced
                this.invokable = null;
                // free the network resources
                releaseResources();

                // free memory resources
                if (invokable != null) {
                    memoryManager.releaseAll(invokable);
                }

                // remove all of the tasks resources
                fileCache.releaseJob(jobId, executionId);

                // close and de-activate safety net for task thread
                LOG.debug("Ensuring all FileSystem streams are closed for task {}", this);
                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

                notifyFinalState();
            } catch (Throwable t) {
                // an error in the resource cleanup is fatal
                String message = String.format("FATAL - exception in resource cleanup of task %s (%s).",
                        taskNameWithSubtask, executionId);
                LOG.error(message, t);
                notifyFatalError(message, t);
            }

            // un-register the metrics at the end so that the task may already be
            // counted as finished when this happens
            // errors here will only be logged
            try {
                metrics.close();
                if (omniTaskMetricGroup != null) {
                    omniTaskMetricGroup.close();
                }
            } catch (Throwable t) {
                LOG.error("Error during metrics de-registration of task {} ({}).", taskNameWithSubtask, executionId,
                        t);
            }
        }
    }

    private void initState() {
        while (true) {
            ExecutionState current = this.executionState;
            if (current == ExecutionState.CREATED) {
                if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
                    // success, we can start our work
                    break;
                }
            } else if (current == ExecutionState.FAILED) {
                // we were immediately failed. tell the TaskManager that we reached our final state
                notifyFinalStateAndCloseMetrics();
                return;
            } else if (current == ExecutionState.CANCELING) {
                if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
                    // we were immediately canceled. tell the TaskManager that we reached our final
                    // state
                    notifyFinalStateAndCloseMetrics();
                    return;
                }
            } else {
                if (metrics != null) {
                    metrics.close();
                }
                throw new IllegalStateException
                        ("Invalid state for beginning of operation of task " + this + '.');
            }
        }
    }
    private void notifyFinalStateAndCloseMetrics() {
        notifyFinalState();
        if (metrics != null) {
            metrics.close();
        }
    }

    private ExecutionConfig beforePrepareEnvironment
            (Map<String, Future<Path>> distributedCacheEntries) throws Exception {
        LOG.debug("Creating FileSystem stream leak safety net for task {}", this);
        FileSystemSafetyNet.initializeSafetyNetForThread();

        // first of all, get a user-code classloader
        // this may involve downloading the job's JAR files and/or classes
        LOG.info("Loading JAR files for task {}.", this);

        userCodeClassLoader = createUserCodeClassloader();
        final ExecutionConfig executionConfig =
                serializedExecutionConfig.deserializeValue(userCodeClassLoader.asClassLoader());

        if (executionConfig.getTaskCancellationInterval() >= 0) {
            // override task cancellation interval from Flink config if set in ExecutionConfig
            taskCancellationInterval = executionConfig.getTaskCancellationInterval();
        }

        if (executionConfig.getTaskCancellationTimeout() >= 0) {
            // override task cancellation timeout from Flink config if set in ExecutionConfig
            taskCancellationTimeout = executionConfig.getTaskCancellationTimeout();
        }

        if (isCanceledOrFailed()) {
            throw new CancelTaskException();
        }

        // ----------------------------------------------------------------
        // register the task with the network stack
        // this operation may fail if the system does not have enough
        // memory to run the necessary data exchanges
        // the registration must also strictly be undone
        // ----------------------------------------------------------------

        LOG.debug("Registering task at network: {}.", this);
        // action 1, natvie should do similary operation
        setupPartitionsAndGates(partitionWriters, inputGates);
        if (jobType == JobType.SQL) {
            bindNativeTaskRefToResultPartition(nativeTaskRef, partitionWriters, jobType);
        }
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
        }
        // action 1 end

        // next, kick off the background copying of files for the distributed cache
        try {
            for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                    DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
                LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
                Future<Path> cp = fileCache.createTmpFile(entry.getKey(), entry.getValue(), jobId, executionId);
                distributedCacheEntries.put(entry.getKey(), cp);
            }
        } catch (Exception e) {
            throw new Exception(String.format("Exception while adding files to distributed cache of task %s (%s).",
                    taskNameWithSubtask, executionId), e);
        }

        if (isCanceledOrFailed()) {
            throw new CancelTaskException();
        }

        return executionConfig;
    }

    private Environment prepareEnvironment(Map<String, Future<Path>> distributedCacheEntries) throws Exception {
        ExecutionConfig executionConfig = beforePrepareEnvironment(distributedCacheEntries);
        // ----------------------------------------------------------------
        //  call the user code initialization methods
        // ----------------------------------------------------------------

        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());

        Environment env = new RuntimeEnvironment(jobId, vertexId, executionId, executionConfig, taskInfo,
                jobConfiguration, taskConfiguration, userCodeClassLoader, memoryManager, ioManager,
                broadcastVariableManager, taskStateManager, aggregateManager, accumulatorRegistry, kvStateRegistry,
                inputSplitProvider, distributedCacheEntries, partitionWriters, inputGates, taskEventDispatcher,
                checkpointResponder, operatorCoordinatorEventGateway, taskManagerConfig, metrics, this,
                externalResourceInfoProvider);

        // Save it so that OmniTaskWrapper can get env for checkpointing
        checkpointingEnv = (RuntimeEnvironment) env;

        // Make sure the user code classloader is accessible thread-locally.
        // We are setting the correct context class loader before instantiating the invokable
        // so that it is available to the invokable during its entire lifetime.
        executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

        StreamConfig streamConfig = new StreamConfig(taskConfiguration);
        InternalOperatorMetricGroup operator = env.getMetricGroup().getOrAddOperator(streamConfig.getOperatorID(),
                streamConfig.getOperatorName());
        // invokale Omni Source Operator Stream Task

        // When constructing invokable, separate threads can be constructed and thus should be
        // monitored for system exit (in addition to invoking thread itself monitored below).
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        LOG.info("the task's invokable is {} ", nameOfInvokableClass);
        return env;
    }

    private void initAliasOfInvokableClass() {
        LOG.info("the task's taskType is {} ", jobType.getValue());
        // tmp add stream task
        if (jobType == JobType.SQL || jobType == JobType.STREAM) {
            StreamConfig streamConfig = new StreamConfig(taskConfiguration);
            StreamOperatorFactory<?> streamOperatorFactory =
                streamConfig.getStreamOperatorFactory(userCodeClassLoader.asClassLoader());
            boolean isKafkaSource = streamOperatorFactory instanceof SourceOperatorFactory
                && ReflectionUtils.retrievePrivateField(
                streamOperatorFactory, "source").getClass().getSimpleName().equals("KafkaSource");
            if (isKafkaSource && nameOfInvokableClass.equals(SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME)) {
                aliasOfInvokableClass = OMNI_SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME;
                LOG.info(" Alias Invokable Class is {}", nameOfInvokableClass);
            } else if (nameOfInvokableClass.equals(SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME)
                || nameOfInvokableClass.equals(SOURCE_STREAM_TASK_CLASS_NAME)) {
                aliasOfInvokableClass = OMNI_SOURCE_STREAM_TASK_CLASS_NAME;
                LOG.info(" Alias Invokable Class is {}", nameOfInvokableClass);
            } else if (nameOfInvokableClass.equals(ONEINPUT_STREAM_TASK_CLASS_NAME)) {
                aliasOfInvokableClass = OMNI_ONEINPUT_STREAM_TASK_CLASS_NAME;
                LOG.info(" Alias Invokable Class is {}", nameOfInvokableClass);
            } else if (nameOfInvokableClass.equals(TWOINPUT_STREAM_TASK_CLASS_NAME)) {
                aliasOfInvokableClass = OMNI_TWOINPUT_STREAM_TASK_CLASS_NAME;
                LOG.info(" Alias Invokable Class is {}", nameOfInvokableClass);
            }
        }
    }

    private TaskInvokable createInitAndInvokeTask(Environment env) throws Throwable {
        initAliasOfInvokableClass();
        TaskInvokable invokable;
        try {
            // now load and instantiate the task's invokable code
            invokable =
                    loadAndInstantiateInvokable(
                            userCodeClassLoader.asClassLoader(), nameOfInvokableClass, env);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }

        // ----------------------------------------------------------------
        //  actual task core work
        // ----------------------------------------------------------------


        // we must make strictly sure that the invokable is accessible to the cancel() call
        // by the time we switched to running.
        this.invokable = invokable;

        // invoke will start the mailbox loop
        // before this invoke, the native stream task and all setup should be ready
        // OmniStreamTask binding to native stream task should be ready as wll

        long nativeStreamTask = 0L;
        boolean useomniFlag = __taskInformation.getTaskConfiguration().getBoolean("useomni", false);
        if (useomniFlag && (jobType == JobType.SQL || jobType == JobType.STREAM)) {
            nativeStreamTask = setupStreamTaskBeforeInvoke(this.nativeTaskRef, this.aliasOfInvokableClass);

            // switch to the INITIALIZING state, if that fails, we have been canceled/failed in the
            // meantime
            // restore original task first
            nativeTaskMetricGroupRef = createNativeTaskMetricGroup(nativeTaskRef);
            // register omni metrics
            omniTaskMetricGroup = registerOmniTaskMetrics();

            if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.INITIALIZING)) {
                throw new CancelTaskException();
            }

            taskManagerActions.updateTaskExecutionState(new TaskExecutionState(executionId,
                    ExecutionState.INITIALIZING));

            // make sure the user code classloader is accessible thread-locally
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

            // create RemoteDataFetcher for remote input channelsE

            originalTaskDataFetcher = createAndStartRemoteDataFetcher(inputGates);

            // call native restore and invoke before java
            long status = doRunRestoreNativeTask(nativeTaskRef, nativeStreamTask);
            if (!transitionState(ExecutionState.INITIALIZING, ExecutionState.RUNNING)) {
                throw new CancelTaskException();
            }
            taskManagerActions.updateTaskExecutionState(new TaskExecutionState(executionId, ExecutionState.RUNNING));
            registerEventDispatcher((StreamTask<?, ?>) invokable);
            status = doRunInvokeNativeTask(nativeTaskRef, nativeStreamTask);
        } else {
            restoreAndInvoke(invokable);
            // mailbox loop ended

            // make sure, we enter the catch block if the task leaves the invoke() method due
            // to the fact that it has been canceled
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  finalization of a successful execution
            // ----------------------------------------------------------------

            // finish the produced partitions. if this fails, we consider the execution failed.
            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                if (partitionWriter != null) {
                    partitionWriter.finish();
                }
            }
        }

        // Task (either java or cpp task) ended

        // try to mark the task as finished
        // if that fails, the task was canceled/failed in the meantime
        if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
            throw new CancelTaskException();
        }
        return invokable;
    }
    public void declineCheckpoint(
            long checkpointID,
            CheckpointFailureReason failureReason,
            @Nullable
            Throwable failureCause) {
        checkpointResponder.declineCheckpoint(
                jobId,
                executionId,
                checkpointID,
                new CheckpointException(
                        "Task name with subtask : " + taskNameWithSubtask,
                        failureReason,
                        failureCause));
    }

    @Override
    public void cancelExecution() {
        super.cancelExecution();
        if (jobType.equals(JobType.SQL)
            && nameOfInvokableClass.equals("org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask")) {
            cancelTask(nativeTaskRef);
        }
        // for stream situation, we could not check which task is native or java
        if (jobType != null && jobType.equals(JobType.STREAM)) {
            cancelTask(nativeTaskRef);
        }
    }

    private void throwableHandle(Throwable t, TaskInvokable invokable) {
        while (true) {
            ExecutionState current = this.executionState;

            if (current == ExecutionState.RUNNING || current == ExecutionState.INITIALIZING
                || current == ExecutionState.DEPLOYING) {
                boolean isBreak = doNonCancelAndNonFailThrowableHandle(t, invokable, current);
                if (isBreak) {
                    break;
                }
            } else if (current == ExecutionState.CANCELING) {
                if (transitionState(current, ExecutionState.CANCELED)) {
                    break;
                }
            } else if (current == ExecutionState.FAILED) {
                // in state failed already, no transition necessary any more
                break;
            } else if (transitionState(current, ExecutionState.FAILED, t)) {
                LOG.error("Unexpected state in task {} ({}) during an exception: {}.",
                        taskNameWithSubtask, executionId, current);
                break;
            }
            // else fall through the loop and
        }
    }

    private boolean doNonCancelAndNonFailThrowableHandle(Throwable t, TaskInvokable invokable, ExecutionState current) {
        if (ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent()) {
            if (transitionState(current, ExecutionState.CANCELED, t)) {
                cancelInvokable(invokable);
                return true;
            }
        } else {
            if (transitionState(current, ExecutionState.FAILED, t)) {
                cancelInvokable(invokable);
                return true;
            }
        }
        return false;
    }

    @Override
    public void deliverOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> evt) throws FlinkException {
        if (jobType != JobType.SQL && jobType != JobType.STREAM) {
            super.deliverOperatorEvent(operator, evt);
            return;
        }
        final TaskInvokable invokable = this.invokable;
        final ExecutionState currentState = this.executionState;

        if (invokable == null
            || (currentState != ExecutionState.RUNNING
            && currentState != ExecutionState.INITIALIZING)) {
            throw new TaskNotRunningException("Task is not running, but in state " + currentState);
        }

        if (invokable instanceof CoordinatedTask) {
            String desc = null;
            try {
                OperatorEvent operatorEvent = evt.deserializeValue(userCodeClassLoader.asClassLoader());
                desc = eventToJsonString(operatorEvent);
                dispatchOperatorEvent(nativeTaskRef, operator.toString(), desc);
            } catch (IOException | ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
                if (getExecutionState() == ExecutionState.RUNNING
                    || getExecutionState() == ExecutionState.INITIALIZING) {
                    FlinkException flinkException = new FlinkException("Error while handling operator event", e);
                    failExternally(flinkException);
                    throw flinkException;
                }
            }
        }
    }

    // The operator event is triggered post-operator initialization
    // We split the native process into two stages,
    // with operator event registration inserted between them.
    private void registerEventDispatcher(StreamTask<?, ?> invokable) {
        operatorEventDispatcher = new OperatorEventDispatcherImpl(
            userCodeClassLoader.asClassLoader(), operatorCoordinatorEventGateway);
        Map<Integer, StreamConfig> transitiveChainedTaskConfigsWithSelf =
            invokable.getConfiguration().getTransitiveChainedTaskConfigsWithSelf(userCodeClassLoader.asClassLoader());
        //
        for (Map.Entry<Integer, StreamConfig> entry : transitiveChainedTaskConfigsWithSelf.entrySet()) {
            StreamConfig streamConfig = entry.getValue();
            if (streamConfig == null) {
                continue;
            }
            StreamOperatorFactory<?> streamOperatorFactory =
                streamConfig.getStreamOperatorFactory(userCodeClassLoader.asClassLoader());
            boolean isKafkaSource = streamOperatorFactory instanceof SourceOperatorFactory
                && ReflectionUtils.retrievePrivateField(
                streamOperatorFactory, "source").getClass().getSimpleName().equals("KafkaSource");
            // Additional operators(e.g. CollectSinkOperator) can perform post-initialization operations at this stage
            if (isKafkaSource) {
                OperatorID operatorID = streamConfig.getOperatorID();
                operatorEventDispatcher.getOperatorEventGateway(operatorID).sendEventToCoordinator(
                    new ReaderRegistrationEvent(
                        getTaskInfo().getIndexOfThisSubtask(),
                        taskManagerConfig.getTaskManagerExternalAddress())
                );
            }
        }
    }

    private List<String> convertToHexStringList(ArrayList<byte[]> splits) {
        List<String> hexStringList = new ArrayList<>();
        for (byte[] bytes : splits) {
            StringBuilder hexString = new StringBuilder();
            for (byte b : bytes) {
                String hex = Integer.toHexString(0xFF & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            hexStringList.add(hexString.toString());
        }
        return hexStringList;
    }

    private String eventToJsonString(OperatorEvent event) throws NoSuchFieldException, IllegalAccessException,
        JsonProcessingException {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        Map<String, Object> fieldMap = new LinkedHashMap<>();
        jsonMap.put("field", fieldMap);
        if (event instanceof WatermarkAlignmentEvent) {
            jsonMap.put("type", "WatermarkAlignmentEvent");
            fieldMap.put("maxWatermark", ((WatermarkAlignmentEvent) event).getMaxWatermark());
        } else if (event instanceof AddSplitEvent) {
            jsonMap.put("type", "AddSplitEvent");
            Field serializerVersion = AddSplitEvent.class.getDeclaredField("serializerVersion");
            Field splits = AddSplitEvent.class.getDeclaredField("splits");
            serializerVersion.setAccessible(true);
            splits.setAccessible(true);
            fieldMap.put("serializerVersion", serializerVersion.get(event));
            ArrayList<byte[]> splitList = (ArrayList<byte[]>) splits.get(event);
            List<String> hexStringList = convertToHexStringList(splitList);
            fieldMap.put("splits", hexStringList);
        } else if (event instanceof SourceEventWrapper) {
            // TODO : currently, do not support HybridSourceReader here
            jsonMap.put("type", "SourceEventWrapper");
        } else if (event instanceof NoMoreSplitsEvent) {
            jsonMap.put("type", "NoMoreSplitsEvent");
        } else {
            throw new IllegalStateException("Received unexpected operator event " + event);
        }
        return objectMapper.writeValueAsString(jsonMap);
    }

    public void bindNativeTask(long nativeTaskAddress) {
        this.nativeTaskRef = nativeTaskAddress;
        bindNativeTaskRefToResultPartition(nativeTaskRef, partitionWriters);
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }
    public boolean isOmniStream() {
        return (this.jobType == JobType.SQL || this.jobType == JobType.STREAM);
    }
    public void omniNotifyCheckpointAborted(
            final long checkpointID, final long latestCompletedCheckpointId) {
        notifyCheckpoint(
                checkpointID, latestCompletedCheckpointId, NotifyCheckpointOperation.ABORT);
    }
    public void omniNotifyCheckpointComplete(final long checkpointID){
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.COMPLETE);
    }
    public void omniNotifyCheckpointSubsumed(long checkpointID) {
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.SUBSUME);
    }

    private void notifyCheckpoint(
            long checkpointId,
            long latestCompletedCheckpointId,
            NotifyCheckpointOperation notifyCheckpointOperation) {
        long ordinal = notifyCheckpointOperation.ordinal();
        if (NotifyCheckpointOperation.ABORT == notifyCheckpointOperation) {
            abortCpp(nativeTaskRef, checkpointId, latestCompletedCheckpointId);
        } else if (NotifyCheckpointOperation.COMPLETE == notifyCheckpointOperation) {
            long inputState = convertExecutionState(executionState);
            completeCpp(nativeTaskRef, checkpointId, inputState);
        } else if (NotifyCheckpointOperation.SUBSUME == notifyCheckpointOperation) {
            subsumedCpp(nativeTaskRef, latestCompletedCheckpointId);
        }
    }

    private OriginalTaskDataFetcher createAndStartRemoteDataFetcher(IndexedInputGate[] inputGates) throws IOException {
        if (isTaskNative()) {
            List<OmniRemoteInputChannel> remoteInputChannels = new ArrayList<>();
            List<OmniLocalInputChannel> localInputChannels = new ArrayList<>();
            for (IndexedInputGate inputGate : inputGates) {
                InputGateWithMetrics inputGateWithMetrics = (InputGateWithMetrics) inputGate;
                
                int numberOfChannels = inputGateWithMetrics.getNumberOfInputChannels();
                
                for (int i = 0; i < numberOfChannels; i++) {
                    InputChannel inputChannel = inputGateWithMetrics.getChannel(i);
                    setRecoverInputStateFutureCompleted((RecoveredInputChannel) inputChannel);
                }
                convertRemoteRecoveryChannelToNormal(inputGateWithMetrics);
                
                for (int i = 0; i < numberOfChannels; i++) {
                    InputChannel inputChannel = inputGateWithMetrics.getChannel(i);
                    if (inputChannel instanceof RemoteInputChannel) {
                        RemoteInputChannel remoteInputChannel = (RemoteInputChannel) inputChannel;
                        remoteInputChannels.add(new OmniRemoteInputChannel(remoteInputChannel));
                    } else
                        if (inputChannel instanceof LocalInputChannel) {
                            LocalInputChannel localInputChannel = (LocalInputChannel) inputChannel;
                            boolean targetIsNative =
                                    checkIfTargetResultPartitionIsNative(localInputChannel.getPartitionId());
                            if (!targetIsNative) {
                                // target task is not native,  but myself is a native task, so we need to create
                                // OmniLocalInputChannel to read data from original java task
                                OmniLocalInputChannel omniLocalInputChannel =
                                        createOmniLocalInputChannel(inputGateWithMetrics, localInputChannel);
                                localInputChannels.add(omniLocalInputChannel);
                            }
                        }
                }
            }
            
            if (!remoteInputChannels.isEmpty() || !localInputChannels.isEmpty()) {
                OriginalTaskDataFetcher originalTaskDataFetcher = new OriginalTaskDataFetcher(nativeTaskRef,
                        this.getTaskInfo().getTaskName(), jobType);
                if (!remoteInputChannels.isEmpty()) {
                    originalTaskDataFetcher.createAndStartRemoteDataFetcher(remoteInputChannels);
                }
                if (!localInputChannels.isEmpty()) {
                    originalTaskDataFetcher.createAndStartLocalDataFetcher(localInputChannels);
                    
                }
                return originalTaskDataFetcher;
            }
            return null;
        } else {
            return null;
        }
    }
    
    boolean checkIfTargetResultPartitionIsNative(ResultPartitionID partitionId) {
        OmniTaskReferenceCounter omniTaskReferenceCounter = taskSlotTable.get(partitionId.getProducerId());
        if (omniTaskReferenceCounter != null) {
            OmniTask omniTask = omniTaskReferenceCounter.getTask();
            return omniTask.isTaskNative();
        } else {
            throw new GeneralRuntimeException("OmniTaskReferenceCounter is null for partitionId: " + partitionId);
        }
        
    }

    private void setRecoverInputStateFutureCompleted(RecoveredInputChannel recoveredInputChannel) {
        Class clazz = recoveredInputChannel.getClass().getSuperclass();
        try {
            Method method = clazz.getDeclaredMethod("getStateConsumedFuture");
            method.setAccessible(true);
            CompletableFuture future = (CompletableFuture) method.invoke(recoveredInputChannel);
            future.complete(null);
        } catch (NoSuchMethodException e) {
            throw new GeneralRuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new GeneralRuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
    }

    private void convertRemoteRecoveryChannelToNormal(InputGateWithMetrics inputGateWithMetrics) {
        try {
            Field inputGateField = InputGateWithMetrics.class.getDeclaredField("inputGate");
            inputGateField.setAccessible(true);
            SingleInputGate singleInputGate = (SingleInputGate) inputGateField.get(inputGateWithMetrics);
            singleInputGate.convertRecoveredInputChannels();
        } catch (NoSuchFieldException e) {
            throw new GeneralRuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
    }

    private SingleInputGate getSingleInputGateFromInputGateWithMetrics(InputGateWithMetrics inputGateWithMetrics) {
        try {
            Field inputGateField = InputGateWithMetrics.class.getDeclaredField("inputGate");
            inputGateField.setAccessible(true);
            return (SingleInputGate) inputGateField.get(inputGateWithMetrics);
        } catch (NoSuchFieldException e) {
            throw new GeneralRuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new GeneralRuntimeException(e);
        }
    }


    private void bindNativeTaskRefToResultPartition(long nativeTaskRef,
            ResultPartitionWriter[] consumableNotifyingPartitionWriters) {
        for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
            BufferWritingResultPartition partition = (BufferWritingResultPartition) partitionWriter;
            partition.setNativeTaskRef(nativeTaskRef);
        }
    }

    private void bindNativeTaskRefToResultPartition(long nativeTaskRef,
                                                    ResultPartitionWriter[] consumableNotifyingPartitionWriters, JobType jobType) {
        for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
            BufferWritingResultPartition partition = (BufferWritingResultPartition) partitionWriter;
            partition.setNativeTaskRef(nativeTaskRef);
            partition.setJobType(jobType);
        }
    }

    public void omniTriggerCheckpointBarrier(
            final long checkpointID,
            final long checkpointTimestamp,
            final CheckpointOptions checkpointOptions){
        this.checkpointOptions = checkpointOptions;
        String checkpointOptionsString=JsonHelper.toJson(checkpointOptions);
        triggerCheckpointCpp(nativeTaskRef,checkpointID,checkpointTimestamp,checkpointOptionsString);
    }

    public RuntimeEnvironment getCheckpointingEnv() {
        return this.checkpointingEnv;
    }

    public ExecutionConfig getExecutionConfig() {
        StreamTask<?, ?> streamTask = (StreamTask<?, ?>) this.invokable;
        return Objects.requireNonNull(streamTask).getExecutionConfig();
    }

    public SnapshotResult<StreamStateHandle> materializeMetaData(
            final long checkpointId,
            final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            final LocalRecoveryConfig localRecoveryConfig,
            final CheckpointOptions checkpointOptions,
            TypeSerializer<?> keySerializer) throws Exception {
        this.checkpointOptions = checkpointOptions;

        final CloseableRegistry snapshotCloseableRegistry = new CloseableRegistry();
        final CloseableRegistry tmpResourcesRegistry = new CloseableRegistry();

        final CheckpointStorageAccess checkpointAccess = ((StreamTask<?, ?>) this.invokable).getEnvironment().getCheckpointStorageAccess();
        this.checkpointStreamFactory = checkpointAccess.resolveCheckpointStorageLocation(checkpointId, this.checkpointOptions.getTargetLocation());

        CheckpointStreamWithResultProvider streamWithResultProvider =
                localRecoveryConfig != null && localRecoveryConfig.isLocalRecoveryEnabled()
                ? CheckpointStreamWithResultProvider.createDuplicatingStream(
                        checkpointId,
                        CheckpointedStateScope.EXCLUSIVE,
                        checkpointStreamFactory,
                        localRecoveryConfig.getLocalStateDirectoryProvider()
                                .orElseThrow(LocalRecoveryConfig.localRecoveryNotEnabled()))
                :CheckpointStreamWithResultProvider.createSimpleStream(
                        CheckpointedStateScope.EXCLUSIVE, checkpointStreamFactory);

        snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

        try {
            keySerializer = (null != keySerializer)
                    ? keySerializer
                    : BasicTypeInfo.STRING_TYPE_INFO.createSerializer(((StreamTask<?, ?>) this.invokable).getExecutionConfig());
            
            KeyedBackendSerializationProxy<?> serializationProxy =
                    new KeyedBackendSerializationProxy<>(keySerializer, stateMetaInfoSnapshots, false);

            final DataOutputView out =
                    new DataOutputViewStreamWrapper(streamWithResultProvider.getCheckpointOutputStream());

            serializationProxy.write(out);

            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                SnapshotResult<StreamStateHandle> result =
                        streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                streamWithResultProvider = null;
                tmpResourcesRegistry.registerCloseable(
                        () -> StateUtil.discardStateObjectQuietly(result));
                LOG.info("materializeMetaData completed, returned stateSize={}", result.getStateSize());
                return result;
            } else {
                throw new IOException("Stream already closed and cannot return a handle.");
            }
        } finally {
            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                IOUtils.closeQuietly(streamWithResultProvider);
            }
        }
    }
    
    public List<HandleAndLocalPath> uploadFilesToCheckpointFs(List<java.nio.file.Path> paths,
                                                              int numberOfSnapshottingThreads) throws Exception {
        if (this.checkpointStreamFactory == null) {
            throw new IllegalStateException("CheckpointStreamFactory is not initialized.");
        }
    
        final CloseableRegistry snapshotCloseableRegistry = new CloseableRegistry();
        final CloseableRegistry tmpResourcesRegistry = new CloseableRegistry();
        final CheckpointedStateScope stateScope = CheckpointedStateScope.SHARED;

        List<HandleAndLocalPath> handles = new ArrayList<>();
        try (RocksDBStateUploader uploader = new RocksDBStateUploader(numberOfSnapshottingThreads)) {
            if (paths == null || paths.isEmpty()) {
                return Collections.emptyList();
            }

            handles =
                    uploader.uploadFilesToCheckpointFs(
                            paths,
                            this.checkpointStreamFactory,
                            stateScope,
                            snapshotCloseableRegistry,
                            tmpResourcesRegistry);
            LOG.info("Checkpoint files uploaded");
        } catch (Throwable t) {
            tmpResourcesRegistry.close();
            LOG.info("Error closing registry", t);
        } finally {
            snapshotCloseableRegistry.close();
        }
        return handles;
    }

    private void stopOmniCreditBasedSequenceNumberingViewReader() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            BufferWritingResultPartition partition = (BufferWritingResultPartition) partitionWriter;
            partition.stopOmniCreditBasedSequenceNumberingViewReader();
        }
    }
    private OmniTaskMetricGroup registerOmniTaskMetrics() {
        return OmniMetricHelper.registerOmniMetrics(this.metrics, nativeTaskMetricGroupRef);
    }
    private boolean isTaskNative(){
        return nativeTaskRef!=0;
    }

    public int getInitialBackoff(Object target) {
        Object res = getFieldByReflection(InputChannel.class, target, "initialBackoff");
        if (res instanceof Integer) {
            return (int) res;
        } else {
            throw new RuntimeException("Failed to access initialBackoff field");
        }
    }

    public int getMaxBackoff(Object target) {
        Object res = getFieldByReflection(InputChannel.class, target, "maxBackoff");
        if (res instanceof Integer) {
            return (int) res;
        } else {
            throw new RuntimeException("Failed to access maxBackoff field");
        }
    }

    public Counter getNumBytesIn(Object target) {
        Object res = getFieldByReflection(InputChannel.class, target, "numBytesIn");
        if (res instanceof Counter) {
            return (Counter) res;
        } else {
            throw new RuntimeException("Failed to access numBytesIn field");
        }
    }
    public Counter getNumBuffersIn(Object target) {
        Object res = getFieldByReflection(InputChannel.class, target, "numBuffersIn");
        if (res instanceof Counter) {
            return (Counter) res;
        } else {
            throw new RuntimeException("Failed to access numBuffersIn field");
        }
    }

    public ResultPartitionManager getPartitionManager(Object target) {
        Object res = getFieldByReflection(LocalInputChannel.class, target, "partitionManager");
        if (res instanceof ResultPartitionManager) {
            return (ResultPartitionManager) res;
        } else {
            throw new RuntimeException("Failed to access partitionManager field");
        }
    }

    public TaskEventPublisher getTaskEventPublisher(Object target) {
        Object res = getFieldByReflection(LocalInputChannel.class, target, "taskEventPublisher");
        if (res instanceof TaskEventPublisher) {
            return (TaskEventPublisher) res;
        } else {
            return null;
        }
    }

    public ChannelStateWriter getChannelStateWriter(Object target) {
        Object res = getFieldByReflection(LocalInputChannel.class, target, "channelStatePersister");
        Object  channelStateWriter = getFieldByReflection(ChannelStatePersister.class, res, "channelStateWriter");
        if (channelStateWriter instanceof ChannelStateWriter) {
            return (ChannelStateWriter) channelStateWriter;
        } else {
            return null;
        }
    }

    public Object getFieldByReflection(Class clazz,Object target,String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return null;
        }
    }
    
    private OmniLocalInputChannel createOmniLocalInputChannel(InputGateWithMetrics inputGateWithMetrics,
            LocalInputChannel localInputChannel) {
        SingleInputGate singleInputGate = getSingleInputGateFromInputGateWithMetrics(inputGateWithMetrics);
        int maxBackoff = getMaxBackoff(localInputChannel);
        int initialBackoff = getInitialBackoff(localInputChannel);
        Counter numBytesIn = getNumBytesIn(localInputChannel);
        Counter numBuffersIn = getNumBuffersIn(localInputChannel);
        ResultPartitionManager partitionManager = getPartitionManager(localInputChannel);
        TaskEventPublisher taskEventPublisher = getTaskEventPublisher(localInputChannel);
        ChannelStateWriter stateWriter = getChannelStateWriter(localInputChannel);
        OmniLocalInputChannel omniLocalInputChannel = new OmniLocalInputChannel(localInputChannel, partitionManager,
                nativeTaskRef, singleInputGate, initialBackoff, maxBackoff, numBytesIn, numBuffersIn,
                taskEventPublisher, stateWriter, this.getTaskInfo().getTaskName());
        return omniLocalInputChannel;
    }
    
    
    public int getConsumers() {
        int consumers = 0;
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            consumers += partitionWriter.getNumberOfSubpartitions();
        }
        return consumers;
    }
    
    private synchronized void deleteParentTaskInSlotTable() {
        for (IndexedInputGate inputGate : this.inputGates) {
            int numOfChannel = inputGate.getNumberOfInputChannels();
            for (int i = 0; i < numOfChannel; i++) {
                InputChannel inputChannel = inputGate.getChannel(i);
                ResultPartitionID partitionId = inputChannel.getPartitionId();
                OmniTaskReferenceCounter omniTaskReferenceCounter = taskSlotTable.get(partitionId.getProducerId());
                if (omniTaskReferenceCounter != null) {
                    omniTaskReferenceCounter.decrementReferenceCount();
                    if (omniTaskReferenceCounter.isReferenceCountZero()) {
                        taskSlotTable.remove(partitionId.getProducerId());
                    }
                }
            }
        }
        if (this.partitionWriters.length == 0) {
            taskSlotTable.remove(this.getExecutionId());
        }
    }

    private long convertExecutionState(ExecutionState inputState) {
        long result = -1;
        switch (inputState) {
            case CREATED:
                result = 0;
                break;
            case SCHEDULED:
                result = 1;
                break;
            case DEPLOYING:
                result = 2;
                break;
            case RUNNING:
                result = 3;
                break;
            case FINISHED:
                result = 4;
                break;
            case CANCELING:
                result = 5;
                break;
            case CANCELED:
                result = 6;
                break;
            case FAILED:
                result = 7;
                break;
            case RECONCILING:
                result = 8;
                break;
            case INITIALIZING:
                result = 9;
                break;
            default:
                break;
        }
        return result;
    }

    public CheckpointStreamWithResultProvider acquireSavepointOutputStream(long checkpointId, CheckpointOptions checkpointOptions) throws Exception {
        this.checkpointOptions = checkpointOptions;
        final CheckpointStorageAccess checkpointAccess = ((StreamTask<?, ?>) this.invokable).getEnvironment().getCheckpointStorageAccess();
        this.checkpointStreamFactory = checkpointAccess.resolveCheckpointStorageLocation(checkpointId, this.checkpointOptions.getTargetLocation());
        return CheckpointStreamWithResultProvider.createSimpleStream(
                CheckpointedStateScope.EXCLUSIVE, checkpointStreamFactory);
    }

    public void writeSavepointOutputStream(CheckpointStreamWithResultProvider provider, byte[] chunk) throws Exception {
        provider.getCheckpointOutputStream().write(chunk);
    }

    public void writeSavepointMetadata(
            CheckpointStreamWithResultProvider provider,
            final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            TypeSerializer<?> keySerializer) throws Exception {

        keySerializer = (null != keySerializer)
                ? keySerializer
                : BasicTypeInfo.STRING_TYPE_INFO.createSerializer(((StreamTask<?, ?>) this.invokable).getExecutionConfig());
        
        KeyedBackendSerializationProxy<?> serializationProxy =
                new KeyedBackendSerializationProxy<>(keySerializer, stateMetaInfoSnapshots, false);
        final DataOutputView out =
                new DataOutputViewStreamWrapper(provider.getCheckpointOutputStream());
        serializationProxy.write(out);
    }

    public long getSavepointOutputStreamPos(CheckpointStreamWithResultProvider provider) throws Exception {
        return provider.getCheckpointOutputStream().getPos();
    }

    public SnapshotResult<StreamStateHandle> closeSavepointOutputStream(CheckpointStreamWithResultProvider provider)
        throws Exception {
        StreamStateHandle handle = (StreamStateHandle)provider
            .closeAndFinalizeCheckpointStreamResult()
            .getJobManagerOwnedSnapshot();
        return SnapshotResult.of(handle);
    }

    // return nativeAddressOfStreamTask
    private native long setupStreamTaskBeforeInvoke(long nativeTaskRef, String StreamTaskClassName /* possible other
     parameter*/);

    private native long doRunRestoreNativeTask(long nativeTaskRef, long streamTaskRef /* possible other parameter*/);

    private native long doRunInvokeNativeTask(long nativeTaskRef, long streamTaskRef /* possible other parameter */);

    private native void dispatchOperatorEvent(long nativeTaskRef, String operatorId, String eventDesc);

    private native long doRunNativeTask(long nativeTaskRef, long streamTaskRef /* possible other parameter */);

    private native long createNativeTaskMetricGroup(long nativeTaskRef);

    private native void triggerCheckpointCpp(long nativeTaskRef,long checkpointID,long checkpointTimestamp,String checkpointOptions);

    public TaskOperatorGatewayWrapper getTaskOperatorGatewayWrapper() {
        return taskOperatorGatewayWrapper;
    }

    public void setTaskOperatorGatewayWrapper(TaskOperatorGatewayWrapper taskOperatorGatewayWrapper) {
        this.taskOperatorGatewayWrapper = taskOperatorGatewayWrapper;
    }
    private native long cancelTask(long nativeTaskRef);
    private native void abortCpp(long nativeTaskRef,long checkpointId,long latestCompletedCheckpointId);
    private native void completeCpp(long nativeTaskRef,long checkpointId, long isRunning);
    private native void subsumedCpp(long nativeTaskRef,long latestCompletedCheckpointId);

}
