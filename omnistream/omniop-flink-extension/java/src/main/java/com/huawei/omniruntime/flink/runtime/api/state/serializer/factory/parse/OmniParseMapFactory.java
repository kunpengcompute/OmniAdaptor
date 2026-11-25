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
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * OmniParseMapFactory
 *
 */

public class OmniParseMapFactory extends OmniParseFactory {

    @Override
    public StateDescriptor<?, ?> buildDescriptorBy(String stateTableName, OmniNativeSerializerJsonInfo info) {
        if (!super.check(stateTableName, info)) {
            return null;
        }
        OmniNativeSerializerJsonInfo keySerializerInfo = info.getKeySerializer();
        if (null == keySerializerInfo) {
            return null;
        }
        OmniNativeSerializerJsonInfo valueSerializerInfo = info.getValueSerializer();
        if (null == valueSerializerInfo) {
            return null;
        }
        TypeInformation<?> keyTypeInfo = buildTypeInformationBy(keySerializerInfo, DEPTH_START);
        if (null == keyTypeInfo) {
            return null;
        }
        TypeInformation<?> valueTypeInfo = buildTypeInformationBy(valueSerializerInfo, DEPTH_START);
        if (null == valueTypeInfo) {
            return null;
        }

        return new MapStateDescriptor<>(stateTableName, keyTypeInfo, valueTypeInfo);
    }
}
