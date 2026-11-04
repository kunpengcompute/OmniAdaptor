package com.omnistream.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * 回退测试用 Top3：与 Top2 逻辑完全相同，仅类名不同（不在 native 白名单内）。
 * 预期：计划 JSON 不带 processFunction 字段，ValidateGroupTableAggOPStrategy 拒绝替换，
 * Java 原生算子执行，输出与 Top2 版本一致。
 *
 * 编译：javac -cp "$FLINK_HOME/lib/*" Top2.java Top3.java
 * 打包：jar cvf top2-udf.jar com/
 */
public class Top3 extends TableAggregateFunction<Tuple2<Long, Integer>, Top3.Top3Accum> {

    public static class Top3Accum {
        public long first;
        public long second;
    }

    @Override
    public Top3Accum createAccumulator() {
        Top3Accum acc = new Top3Accum();
        acc.first = Long.MIN_VALUE;
        acc.second = Long.MIN_VALUE;
        return acc;
    }

    public void accumulate(Top3Accum acc, Long v) {
        if (v != null && v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v != null && v > acc.second) {
            acc.second = v;
        }
    }

    public void emitValue(Top3Accum acc, Collector<Tuple2<Long, Integer>> out) {
        // 与 Top2（参考样例）完全一致：只输出有值的名次，空名次不发。
        if (acc.first != Long.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Long.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }
}
