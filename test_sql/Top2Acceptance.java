/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.omnistream.acceptance;

import java.time.ZoneId;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * GroupTableAggregate (Top2) acceptance job -- Table API form.
 *
 * <p>Why Table API: Flink 1.16 removed the SQL entry for table-aggregate UDFs
 * ("LATERAL TABLE(tableAgg) GROUP BY" fails vanilla validation), while the Table
 * API flatAggregate is the official entry and reaches the exact same
 * GroupTableAggregate operator that the native whitelist (Top2) replaces.
 *
 * <p>Modes (first program argument):
 * <ul>
 *   <li>run      (default) Top2 on the heap backend: print EXPLAIN, then execute.</li>
 *   <li>explain  Top2: print EXPLAIN only (no sink, no job submission).</li>
 *   <li>top3     fallback check: same logic under the Top3 class (not whitelisted,
 *               the operator must keep the Java path).</li>
 *   <li>rocksdb  dual-backend check: Top2 on the RocksDB state backend.</li>
 * </ul>
 *
 * <p>Everything else mirrors the SQL-era test files: the same bounded CSV source
 * (top2_bid.csv, 7 rows) and the same print sink; the expected changelog is the
 * 20-row sequence produced by the vanilla engine (documented in the test README).
 *
 * <p>Build (on the build machine, inside /opt/buildtools/top2_udf):
 * <pre>
 *   javac -encoding UTF-8 -cp "/opt/flink-1.16.3/lib/*:top2-udf.jar" \
 *       -d . com/omnistream/acceptance/Top2Acceptance.java
 *   jar uf top2-udf.jar com/omnistream/acceptance
 * </pre>
 *
 * <p>Run (the job jar is distributed to the cluster, so the UDF classes travel with it):
 * <pre>
 *   flink run -c com.omnistream.acceptance.Top2Acceptance /opt/buildtools/top2_udf/top2-udf.jar run
 * </pre>
 */
public final class Top2Acceptance {

    private static final String SRC_PATH =
            "file:///opt/notebook_my/install_script/queries/csv_test/GroupTableAggregate/top2_bid.csv";

    private Top2Acceptance() {
    }

    public static void main(String[] args) throws Exception {
        final String mode = args.length > 0 ? args[0] : "run";
        final boolean explainOnly = "explain".equals(mode);
        final boolean useTop3 = "top3".equals(mode);
        final boolean useRocksdb = "rocksdb".equals(mode);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        if (useRocksdb) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
        }

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // Same session settings as the SQL-era test files.
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "false");

        // ---- DDL: bounded CSV source (the shared acceptance dataset) ----
        tEnv.executeSql(
                "CREATE TABLE src (\n"
                        + "  bidder BIGINT,\n"
                        + "  price  BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'path' = '" + SRC_PATH + "',\n"
                        + "  'format' = 'csv',\n"
                        + "  'csv.null-literal' = 'null'\n"
                        + ")");

        // ---- DDL: print sink (the native WRITE_TO_FILE hook prints to /tmp/flink_output.txt) ----
        // rank is INT: the Top2 UDF emits Tuple2<Long, Integer>, so the raw flatAggregate
        // schema aligns with the sink column-for-column and the planner needs NO Calc at all.
        // (A select(...).cast() here would insert a Calc whose Table-API AS / CAST RexNodes
        // are flagged INVALID by RexNodeUtil and would veto the whole operator chain.)
        tEnv.executeSql(
                "CREATE TABLE top2_result (\n"
                        + "  bidder BIGINT,\n"
                        + "  `value` BIGINT,\n"
                        + "  `rank`  INT\n"
                        + ") WITH ('connector' = 'print')");

        // ---- UDF: the whitelist matches the CLASS simple name (Top2 vs Top3), not the
        // registration name, so the top3 mode exercises the fallback path. ----
        final String udfName;
        if (useTop3) {
            tEnv.createTemporarySystemFunction("top3", com.omnistream.udf.Top3.class);
            udfName = "top3";
        } else {
            tEnv.createTemporarySystemFunction("top2", com.omnistream.udf.Top2.class);
            udfName = "top2";
        }

        // ---- The table-aggregate query: the Table API acceptance shape ----
        // from(src).groupBy(bidder).flatAggregate(top2(price) as value, rank).select(...)
        // is the official 1.16 entry for "LATERAL TABLE(top2(price)) ... GROUP BY bidder".
        // select() is mandatory to close a FlatAggregateTable; it emits AS($i, 'name')
        // RexNodes for the plain column references, which RexNodeUtil transparently
        // forwards (the native engine evaluates the aliased operand). No CAST: the sink's
        // rank column is INT, matching the UDF's Tuple2<Long, Integer> output natively.
        final Table result = tEnv.from("src")
                .groupBy($("bidder"))
                .flatAggregate(call(udfName, $("price")).as("value", "rank"))
                .select($("bidder"), $("value"), $("rank"));

        // ---- EXPLAIN: expect a GroupTableAggregate operator (and, in the Top2 plan,
        // the native description JSON with processFunction=GroupTableAggregateFunction). ----
        System.out.println("=========== EXPLAIN (mode=" + mode + ") ===========");
        System.out.println(result.explain());
        System.out.println("=========== END EXPLAIN ===========");

        if (!explainOnly) {
            // Bounded source: await() returns once the changelog has drained.
            result.executeInsert("top2_result").await();
            System.out.println("=========== job finished (mode=" + mode + ") ===========");
        }
    }
}
