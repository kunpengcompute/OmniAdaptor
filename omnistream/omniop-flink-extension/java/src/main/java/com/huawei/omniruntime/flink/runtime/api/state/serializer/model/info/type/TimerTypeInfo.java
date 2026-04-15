package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.type;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.util.Preconditions;

public class TimerTypeInfo<K, N> extends TypeInformation<TimerHeapInternalTimer<K, N>> {
    private final TypeInformation<K> keyTypeInfo;
    private final TypeInformation<N> namespaceTypeInfo;

    public TimerTypeInfo(TypeInformation<K> keyTypeInfo, TypeInformation<N> namespaceTypeInfo) {
        this.keyTypeInfo = (TypeInformation) Preconditions.checkNotNull(keyTypeInfo, "The key type information cannot be null.");
        this.namespaceTypeInfo = (TypeInformation) Preconditions.checkNotNull(namespaceTypeInfo, "The namespace type information cannot be null.");
    }

    public TimerTypeInfo(Class<K> keyClass, Class<N> namespaceClass) {
        this.keyTypeInfo = of((Class) Preconditions.checkNotNull(keyClass, "The key class cannot be null."));
        this.namespaceTypeInfo = of((Class) Preconditions.checkNotNull(namespaceClass, "The namespace class cannot be null."));
    }

    public TypeInformation<K> getKeyTypeInfo() {
        return this.keyTypeInfo;
    }

    public TypeInformation<N> getValueTypeInfo() {
        return this.namespaceTypeInfo;
    }

    public boolean isBasicType() {
        return false;
    }

    public boolean isTupleType() {
        return false;
    }

    public int getArity() {
        return 0;
    }

    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<TimerHeapInternalTimer<K, N>> getTypeClass() {
        return (Class<TimerHeapInternalTimer<K, N>>) (Class<?>) TimerHeapInternalTimer.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<TimerHeapInternalTimer<K, N>> createSerializer(ExecutionConfig config) {
        TypeSerializer<K> keyTypeSerializer = this.keyTypeInfo.createSerializer(config);
        TypeSerializer<N> namespaceTypeSerializer = this.namespaceTypeInfo.createSerializer(config);
        return new TimerSerializer<>(keyTypeSerializer, namespaceTypeSerializer);
    }

    @Override
    public String toString() {
        return "TimerHeapInternalTimer<" + this.keyTypeInfo + ", " + this.namespaceTypeInfo + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof TimerTypeInfo)) {
            return false;
        } else {
            TimerTypeInfo<K, N> other = (TimerTypeInfo) obj;
            return other.canEqual(this) && this.keyTypeInfo.equals(other.keyTypeInfo) && this.namespaceTypeInfo.equals(other.namespaceTypeInfo);
        }
    }

    public int hashCode() {
        return 31 * this.keyTypeInfo.hashCode() + this.namespaceTypeInfo.hashCode();
    }

    public boolean canEqual(Object obj) {
        return obj != null && obj.getClass() == this.getClass();
    }
}
