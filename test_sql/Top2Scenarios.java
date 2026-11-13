/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.omnistream.acceptance;

import java.time.ZoneId;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * GroupTableAggregate (Top2) multi-scenario acceptance job -- Table API form.
 *
 * <p>One job run = one scenario = one bounded CSV dataset (same directory as the
 * base top2_bid.csv). The query, UDF and sink are identical to Top2Acceptance;
 * only the input data changes, so each scenario isolates one behavior of the
 * Top2 changelog semantics that the C++ unit tests cover but the base 7-row
 * dataset does not:
 *
 * <ul>
 *   <li>single   1 row, 1 group  -- minimal end-to-end path (first-row INSERT only).</li>
 *   <li>down     5 rows DESC     -- after the board fills, every further row still
 *                                   re-emits the WHOLE board (-D all + +I all):
 *                                   vanilla never diffs the board, it re-sends it.</li>
 *   <li>dupfirst 5,5 then 8      -- a duplicate as the group's FIRST value occupies
 *                                   both slots (equal value settles one slot lower).</li>
 *   <li>groups   3 groups interleaved -- per-group keyed state isolation across
 *                                   interleaved input rows.</li>
 *   <li>large    60 rows, 2 groups -- stability/volume smoke test.</li>
 * </ul>
 *
 * <p>Expected changelog row counts (vanilla semantics, documented in README):
 * single=1, down=16, dupfirst=8, groups=16, large=234. Acceptance criterion is
 * native-vs-vanilla diff EMPTY per scenario (values, RowKinds and row order).
 *
 * <p>Usage:
 * <pre>
 *   flink run -c com.omnistream.acceptance.Top2Scenarios top2-udf.jar &lt;scenario&gt; [explain]
 * </pre>
 * The optional second argument "explain" prints the plan without submitting.
 */
public final class Top2Scenarios {

    private static final String CSV_DIR =
            "file:///opt/notebook_my/install_script/queries/csv_test/GroupTableAggregate/";

    private Top2Scenarios() {
    }

    public static void main(String[] args) throws Exception {
        final String scenario = args.length > 0 ? args[0] : "single";
        final boolean explainOnly = args.length > 1 && "explain".equals(args[1]);

        final String csvFile;
        switch (scenario) {
            case "single":
                csvFile = "top2_single.csv";
                break;
            case "down":
                csvFile = "top2_down.csv";
                break;
            case "dupfirst":
                csvFile = "top2_dupfirst.csv";
                break;
            case "groups":
                csvFile = "top2_groups.csv";
                break;
            case "large":
                csvFile = "top2_large.csv";
                break;
            default:
                System.err.println(
                        "unknown scenario '" + scenario + "'; expected one of: single | down | dupfirst | groups | large");
                System.exit(1);
                return; // unreachable, keeps javac happy about csvFile
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "false");

        // ---- DDL: bounded CSV source (the scenario dataset) ----
        tEnv.executeSql(
                "CREATE TABLE src (\n"
                        + "  bidder BIGINT,\n"
                        + "  price  BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'path' = '" + CSV_DIR + csvFile + "',\n"
                        + "  'format' = 'csv',\n"
                        + "  'csv.null-literal' = 'null'\n"
                        + ")");

        // ---- DDL: print sink (native WRITE_TO_FILE hook prints to /tmp/flink_output.txt) ----
        tEnv.executeSql(
                "CREATE TABLE top2_result (\n"
                        + "  bidder BIGINT,\n"
                        + "  `value` BIGINT,\n"
                        + "  `rank`  INT\n"
                        + ") WITH ('connector' = 'print')");

        // ---- UDF: whitelist matches the CLASS simple name (Top2), not the registration name. ----
        tEnv.createTemporarySystemFunction("top2", com.omnistream.udf.Top2.class);

        // ---- Same acceptance query as Top2Acceptance, only the data differs. ----
        final Table result = tEnv.from("src")
                .groupBy($("bidder"))
                .flatAggregate(call("top2", $("price")).as("value", "rank"))
                .select($("bidder"), $("value"), $("rank"));

        System.out.println("=========== EXPLAIN (scenario=" + scenario + ", data=" + csvFile + ") ===========");
        System.out.println(result.explain());
        System.out.println("=========== END EXPLAIN ===========");

        if (!explainOnly) {
            result.executeInsert("top2_result").await();
            System.out.println("=========== job finished (scenario=" + scenario + ") ===========");
        }
    }
}
