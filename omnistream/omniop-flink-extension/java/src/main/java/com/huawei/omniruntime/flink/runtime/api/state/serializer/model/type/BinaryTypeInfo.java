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

package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.type;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.util.Preconditions;

import java.util.List;

public class BinaryTypeInfo<T> extends TypeInformation<T> {

    private T t;
    private TypeSerializer<T> typeSerializer;

    public BinaryTypeInfo(T t, TypeSerializer<T> typeSerializer) {
        this.t = Preconditions.checkNotNull(t, "The T obj cannot be null.");
        this.typeSerializer = Preconditions.checkNotNull(typeSerializer, "The typeSerializer cannot be null.");
    }

    public static BinaryTypeInfo<BinaryRowData> of(BinaryRowData binaryRowData, List<String> inputTypes) {
        return new BinaryTypeInfo<>(binaryRowData, new BinaryRowDataSerializer(binaryRowData.getArity(), inputTypes));
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 0;
    }

    @Override
    public Class<T> getTypeClass() {
        return (Class<T>) t.getClass();
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return typeSerializer;
    }

    @Override
    public String toString() {
        return t.getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof BinaryTypeInfo)) {
            return false;
        } else {
            BinaryTypeInfo<T> other = (BinaryTypeInfo) obj;
            return other.canEqual(this) && this.t.equals(other.t);
        }
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj != null && obj.getClass() == this.getClass();
    }
}
