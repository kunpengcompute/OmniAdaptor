package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
