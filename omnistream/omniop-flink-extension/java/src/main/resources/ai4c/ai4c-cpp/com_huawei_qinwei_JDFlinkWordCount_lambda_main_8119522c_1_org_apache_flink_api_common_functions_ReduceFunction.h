#ifndef COM_HUAWEI_QINWEI_JDFLINKWORDCOUNT_LAMBDA_MAIN_8119522C_1_ORG_APACHE_FLINK_API_COMMON_FUNCTIONS_REDUCEFUNCTION_H
#define COM_HUAWEI_QINWEI_JDFLINKWORDCOUNT_LAMBDA_MAIN_8119522C_1_ORG_APACHE_FLINK_API_COMMON_FUNCTIONS_REDUCEFUNCTION_H

#include "basictypes/ClassRegistry.h"
#include "basictypes/Long.h"
#include "basictypes/Object.h"
#include "basictypes/StringConstant.h"
#include "basictypes/Tuple2.h"
#include "functions/ReduceFunction.h"
#include "nlohmann/json.hpp"


class com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction : public ReduceFunction<Object> {
public:
    com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction() = default;
    ~com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction();
    Object *reduce(Object *input0, Object *input1)  override;
    com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction(nlohmann::json jsonObj);
};

#endif
