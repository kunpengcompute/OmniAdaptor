/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import java.util.HashMap;
import java.util.Map;

public class ValidateOperatorStrategyFactory {

    private static final Map<String, AbstractValidateOperatorStrategy> strategyMap = new HashMap<>();

    static {
        strategyMap.put("GroupAggregate", new ValidateAggOPStrategy());
        strategyMap.put("WindowAggregate", new ValidateWindowAggOPStrategy());
        strategyMap.put("LocalWindowAggregate", new ValidateWindowAggOPStrategy());
        strategyMap.put("GlobalWindowAggregate", new ValidateWindowAggOPStrategy());
        strategyMap.put("GroupWindowAggregate", new ValidateGroupWindowAggOPStrategy());
        strategyMap.put("IncrementalGroupAggregate", new ValidateAggOPStrategy());
        strategyMap.put("Join", new ValidateJoinOPStrategy());
        strategyMap.put("WindowJoin", new ValidateWindowJoinOPStrategy());
        strategyMap.put("LookupJoin", new ValidateLookupJoinOPStrategy());
        strategyMap.put("Calc", new ValidateCalcOPStrategy());
        strategyMap.put("Expand", new ValidateExpandOPStrategy());
        strategyMap.put("Deduplicate", new ValidateDeduplicateOPStrategy());
        strategyMap.put("ChangelogNormalize", new ValidateChangelogNormalizeOPStrategy());
        strategyMap.put("WatermarkAssigner", new ValidateWatermarkOPStrategy());
        strategyMap.put("Rank", new ValidateRankOPStrategy());
        strategyMap.put("Limit", new ValidateLimitOPStrategy());
        strategyMap.put("WindowTableFunction", new ValidateWindowTableFunctionOPStrategy());
    }

    public static AbstractValidateOperatorStrategy getStrategy(String operatorName) {
        return strategyMap.getOrDefault(operatorName, new ValidateDefaultOPStrategy());
    }
}
