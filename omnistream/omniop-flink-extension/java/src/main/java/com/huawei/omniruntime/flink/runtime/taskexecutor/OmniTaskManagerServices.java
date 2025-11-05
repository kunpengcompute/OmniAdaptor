/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.omniruntime.flink.runtime.taskexecutor;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.graph.json.TaskManagerServicesConfigurationPOJO;
import com.huawei.omniruntime.flink.runtime.shuffle.OmniShuffleEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// one of the top level OmniStream native Object
/**
 * OmniTaskManagerServices
 *
 * @since 2025-04-27
 */
public class OmniTaskManagerServices {
    private static final Logger LOG = LoggerFactory.getLogger(OmniTaskManagerServices.class);

    private long nativeOmniTaskManagerServicesAddress;

    private OmniShuffleEnvironment omniShuffleEnvironment;

    public OmniTaskManagerServices(TaskManagerServicesConfigurationPOJO taskManagerServicesConfiguration) {
        LOG.info("taskManagerServicesConfiguration is {}", taskManagerServicesConfiguration);
        LOG.info("taskManagerServicesConfiguration JSON is {}", JsonHelper.toJson(taskManagerServicesConfiguration));
        this.nativeOmniTaskManagerServicesAddress = createOmniTaskManagerServices(
                JsonHelper.toJson(taskManagerServicesConfiguration));
    }

    /**
     * fromConfiguration
     *
     * @param taskManagerServicesConfiguration taskManagerServicesConfiguration
     * @return OmniTaskManagerServices
     * @throws Exception Exception
     */
    public static OmniTaskManagerServices fromConfiguration(
            TaskManagerServicesConfigurationPOJO taskManagerServicesConfiguration)
            throws Exception {
        return new OmniTaskManagerServices(taskManagerServicesConfiguration);
    }

    private static native long createOmniTaskManagerServices(String taskManagerServicesConfiguration);

    public long getNativeOmniTaskManagerServicesAddress() {
        return nativeOmniTaskManagerServicesAddress;
    }
}
