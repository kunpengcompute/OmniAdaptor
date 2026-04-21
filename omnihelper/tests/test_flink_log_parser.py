"""
   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
            http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.
"""

import os
import argparse
import shutil
import unittest

from omnihelper.flink_log_parser import FlinkLogParser


class TestFlinkLogParser(unittest.TestCase):
    """测试 FlinkLogParser 类的测试用例"""

    def setUp(self):
        """初始化测试环境"""
        self.test_data = [
            {
                'jobid': 'job-123',
                'taskid': 'task-456',
                '状态': 'RUNNING',
                '算子名称': 'Map',
                'Input': 'test_input_1',
                'Output': 'test_output_1',
                '出现频次': 5,
                '运行时间(s)': 1.23,
                '输入数据量': '100MB',
                '输出数据量': '50MB',
                '表达式/内置函数名称': 'SUBSTRING',
                '表达式Input': 'col1, 1, 5',
                '嵌套内容': 'SUBSTRING(col1, 1, 5)',
                '表达式出现频次': 3
            },
            {
                'jobid': 'job-123',
                'taskid': 'task-456',
                '状态': 'RUNNING',
                '算子名称': 'Filter',
                'Input': 'test_input_1',
                'Output': 'test_output_2',
                '出现频次': 3,
                '运行时间(s)': 0.89,
                '输入数据量': '50MB',
                '输出数据量': '30MB',
                '表达式/内置函数名称': 'WHERE',
                '表达式Input': 'col2 > 10',
                '嵌套内容': 'WHERE col2 > 10',
                '表达式出现频次': 2
            },
            {
                'jobid': 'job-123',
                'taskid': 'task-789',
                '状态': 'FINISHED',
                '算子名称': 'Reduce',
                'Input': 'test_output_2',
                'Output': 'test_output_3',
                '出现频次': 2,
                '运行时间(s)': 2.45,
                '输入数据量': '30MB',
                '输出数据量': '10MB',
                '表达式/内置函数名称': 'SUM',
                '表达式Input': 'col3',
                '嵌套内容': 'SUM(col3)',
                '表达式出现频次': 1
            },
            {
                'jobid': 'job-456',
                'taskid': 'task-123',
                '状态': 'RUNNING',
                '算子名称': 'Join',
                'Input': 'test_input_2, test_input_3',
                'Output': 'test_output_4',
                '出现频次': 1,
                '运行时间(s)': 3.67,
                '输入数据量': '200MB',
                '输出数据量': '150MB',
                '表达式/内置函数名称': '',
                '表达式Input': '',
                '嵌套内容': '',
                '表达式出现频次': ''
            },
            {
                'jobid': 'job-456',
                'taskid': 'task-123',
                '状态': 'RUNNING',
                '算子名称': '',
                'Input': '',
                'Output': '',
                '出现频次': '',
                '运行时间(s)': '',
                '输入数据量': '',
                '输出数据量': '',
                '表达式/内置函数名称': 'CONCAT',
                '表达式Input': 'col1, col2',
                '嵌套内容': 'CONCAT(col1, col2)',
                '表达式出现频次': 4
            }
        ]

        self.args = argparse.Namespace(
            host='127.0.0.1',
            port=8081,
            interval=1000,
            timeout=30,
            use_https=False,
            ssl_verify=True,
            output_dir=None,
            show_op_details=True
        )
        self.output_dir = "./tmp_output"
        self.args.output_dir = self.output_dir

    def tearDown(self):
        """清理测试环境"""
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

    def test_flink_report_generation(self):
        """测试 Flink 报告生成功能"""
        flink_parser = FlinkLogParser(self.args)
        flink_parser.analysis_result = self.test_data
        flink_parser.generate_report()

        output_files = os.listdir(self.output_dir)
        self.assertGreater(len(output_files), 0, "No report file generated")


if __name__ == "__main__":
    unittest.main()
