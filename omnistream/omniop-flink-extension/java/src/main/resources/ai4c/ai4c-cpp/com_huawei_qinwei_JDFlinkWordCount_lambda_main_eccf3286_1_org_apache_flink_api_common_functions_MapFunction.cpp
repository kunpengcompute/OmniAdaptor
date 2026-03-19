#include "com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction.h"
#include <charconv>
com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction::com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction(nlohmann::json jsonObj)
 {
    strBuffer = new String();
    longBuffer= new Long();
	result = new Tuple2(strBuffer, longBuffer);
}
com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction::~com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction()
{
    strBuffer->putRefCount();
    longBuffer->putRefCount();
	result->putRefCount();
}


Object *com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction::map(Object *obj)
{
    auto buffer = reinterpret_cast<String *>(obj);
    char *data = buffer->getData();
    int64_t size = buffer->getSize();
    std::string_view origin_str(data, size);
    const std::vector<std::string_view> &strings = split(origin_str, " ");

    strBuffer->setValue(strings[0]);
    int64_t value = 0;
    auto [ptr, ec] = std::from_chars(strings[1].data(), strings[1].data() + strings[1].size(), value);
    longBuffer->setValue(value);

    result->getRefCount();
    result->Set(strBuffer, longBuffer);
    return result;
}
