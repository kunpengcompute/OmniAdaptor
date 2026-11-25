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

#include "com_huawei_qinwei_JDFlinkWordCount_lambda_main_b9058687_1_org_apache_flink_api_java_functions_KeySelector.h"
com_huawei_qinwei_JDFlinkWordCount_lambda_main_b9058687_1_org_apache_flink_api_java_functions_KeySelector::com_huawei_qinwei_JDFlinkWordCount_lambda_main_b9058687_1_org_apache_flink_api_java_functions_KeySelector(nlohmann::json jsonObj)
 {
}


Object *com_huawei_qinwei_JDFlinkWordCount_lambda_main_b9058687_1_org_apache_flink_api_java_functions_KeySelector::getKey(Object *obj) {
    Tuple2 *r2 = reinterpret_cast<Tuple2 *>(obj);

    Object *r0 = nullptr;
    String *r1 = nullptr;

    r0 = r2->f0;
    r1 = reinterpret_cast<String *>(r0);
    if (r1 != nullptr) {
        r1->getRefCount();
    }

    return r1;
}
