#include "com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction.h"
#include "basictypes/StringView.h"

com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction::com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction(nlohmann::json jsonObj)
 {
	 output.resize(64);
	 result = new StringView();
}

com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction::~com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction()
{
	result->putRefCount();
}

Object *com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction::map(Object *obj)
{
	Tuple2 *tupleInfo = reinterpret_cast<Tuple2 *>(obj);

    auto f0 = tupleInfo->f0;
    auto f0_p = reinterpret_cast<String *>(f0)->getData();
    auto f0_s = reinterpret_cast<String *>(f0)->getSize();

    auto f1 = tupleInfo->f1;
    int64_t f1_val = reinterpret_cast<Long *>(f1)->getValue();
    std::string f1_str = std::to_string(f1_val);

    auto f1_p = f1_str.data();
    auto f1_s = f1_str.size();
    auto totalLen = f0_s + f1_s + 1;
    if (output.length() < totalLen) {
        output.resize(totalLen);
    }

    std::memcpy(output.data(), f0_p, f0_s);
    output[f0_s] = ' ';
    std::memcpy(output.data() + f0_s + 1, f1_p, f1_s);

	result->getRefCount();
	result->setData(output.data());
	result->setSize(totalLen);
	return result;
}

