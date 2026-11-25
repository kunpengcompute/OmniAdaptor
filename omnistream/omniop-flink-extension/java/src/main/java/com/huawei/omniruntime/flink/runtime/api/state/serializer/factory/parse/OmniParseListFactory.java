/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * OmniAdaptor is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * OmniParseListFactory
 *
 */

public class OmniParseListFactory extends OmniParseFactory {

    @Override
    public StateDescriptor<?, ?> buildDescriptorBy(String stateTableName, OmniNativeSerializerJsonInfo info) {
        if (!super.check(stateTableName, info)) {
            return null;
        }
        OmniNativeSerializerJsonInfo valueSerializerInfo = info.getValueSerializer();
        if (null == valueSerializerInfo) {
            return null;
        }
        TypeInformation<?> elementTypeInfo = buildTypeInformationBy(valueSerializerInfo, DEPTH_START);
        if (null == elementTypeInfo) {
            return null;
        }

        return new ListStateDescriptor<>(stateTableName, elementTypeInfo);
    }
}
