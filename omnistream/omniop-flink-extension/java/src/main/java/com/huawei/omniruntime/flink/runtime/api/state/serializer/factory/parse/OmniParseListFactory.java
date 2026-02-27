package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
