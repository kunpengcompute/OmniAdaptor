package com.omnistream.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * 验收用 Top2（BIGINT/Long 版）。
 * 语义与参考样例 personal/group算子支持udf.java 完全一致，仅把参数/状态类型从
 * Integer 放大为 Long，以匹配 v1 native 白名单的 BIGINT 排序键
 * （SQL 里 LATERAL TABLE(top2(CAST(price AS BIGINT)))）。
 *
 * 注意：类简单名必须是 Top2（native 白名单按类名点名，注册名 top2 无关）。
 * 编译：javac -cp "$FLINK_HOME/lib/*" Top2.java Top3.java
 * 打包：jar cvf top2-udf.jar com/
 */
public class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2.Top2Accum> {

    /** 榜单状态：每组维护最大的两个值。public static 便于 Flink codegen 跨包访问。 */
    public static class Top2Accum {
        public long first;
        public long second;
    }

    @Override
    public Top2Accum createAccumulator() {
        Top2Accum acc = new Top2Accum();
        acc.first = Long.MIN_VALUE;
        acc.second = Long.MIN_VALUE;
        return acc;
    }

    public void accumulate(Top2Accum acc, Long v) {
        if (v != null && v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v != null && v > acc.second) {
            acc.second = v;
        }
    }

    public void emitValue(Top2Accum acc, Collector<Tuple2<Long, Integer>> out) {
        // 与参考样例（group算子支持udf.java）及 C++ 实现对齐：
        // 只输出有值的名次，空名次不发（MIN_VALUE 是"无值"哨兵）。
        if (acc.first != Long.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Long.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }
}
