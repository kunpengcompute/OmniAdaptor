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

import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.util.Preconditions;

public class WindowTypeInfo<T extends Window> extends TypeInformation<T> {
    private T t;
    private TypeSerializer<T> typeSerializer;

    public WindowTypeInfo(T t, TypeSerializer typeSerializer) {
        this.t = Preconditions.checkNotNull(t, "The T obj cannot be null.");
        this.typeSerializer = Preconditions.checkNotNull(typeSerializer, "The typeSerializer cannot be null.");
    }

    public static <T extends Window> WindowTypeInfo<T> of(T t) {
        Preconditions.checkNotNull(t, "The T obj cannot be null.");
        if (t instanceof TimeWindow) {
            return new WindowTypeInfo<>(t, new TimeWindow.Serializer());
        } else if (t instanceof CountWindow) {
            return new WindowTypeInfo<>(t, new CountWindow.Serializer());
        } else {
            throw new GeneralRuntimeException("The type of a Window object is not supported.");
        }
    }

    public static <T extends Window> WindowTypeInfo<T> ofClass(Class<?> clazz) {
        Preconditions.checkNotNull(clazz, "The clazz cannot be null.");
        if (clazz == TimeWindow.class) {
            return (WindowTypeInfo<T>) of(new TimeWindow(0, 1));
        } else if (clazz == CountWindow.class) {
            return (WindowTypeInfo<T>)of(new CountWindow(0));
        } else {
            throw new GeneralRuntimeException("The type of a Window object is not supported.");
        }
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
        } else if (!(obj instanceof WindowTypeInfo)) {
            return false;
        } else {
            WindowTypeInfo<T> other = (WindowTypeInfo) obj;
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
