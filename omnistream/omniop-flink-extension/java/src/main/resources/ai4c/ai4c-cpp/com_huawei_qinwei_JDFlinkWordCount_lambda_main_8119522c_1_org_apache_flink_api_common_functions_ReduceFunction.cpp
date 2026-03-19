#include "com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction.h"
com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction::com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction(nlohmann::json jsonObj)
{}

com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction::~com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction()
{}


Object *com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction::reduce(Object *t1, Object *t2)
{
	auto *tuple1 = reinterpret_cast<Tuple2*>(t1);
	auto *f0 = tuple1->f0;
    auto *f1 = tuple1->f1;
    int64_t ret1 = reinterpret_cast<Long *>(f1)->getValue();

    auto *tuple2 = reinterpret_cast<Tuple2*>(t2);
    auto *f2 = tuple2->f1;
    int64_t ret2 = reinterpret_cast<Long *>(f2)->getValue();

    auto ret3 = std::max(ret1, ret2) + 1;

    auto *buffer = Long::valueOf(ret3);
    auto *tuple = new Tuple2(f0, buffer);
    buffer->putRefCount();
    return tuple;
}
