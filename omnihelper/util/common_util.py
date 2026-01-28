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
import sys
import platform


class CommonUtil:
    @staticmethod
    def get_execute_path():
        if getattr(sys, 'frozen', False):
            return os.path.dirname(os.path.realpath(sys.executable))
        return os.path.dirname(__file__)

    @staticmethod
    def print_failed_files(failed_files, all_files_length):
        # 统一展示失败结果
        if failed_files:
            print(f"Processing completed with {len(failed_files)} failed file{'s' if len(failed_files) != 1 else ''}:")
            for i, (filepath, error) in enumerate(failed_files, 1):
                print(f"{i}. [File]: {filepath}")
                print(f"   [Error]: {error}")

            # 所有文件都处理失败
            if all_files_length == len(failed_files):
                print(f"\nAll {all_files_length} file{'s' if all_files_length != 1 else ''} processed failed!")
            else:
                print(f"\nSummary: {all_files_length - len(failed_files)} succeeded, {len(failed_files)} failed")
        else:
            print(f"All {all_files_length} file{'s' if all_files_length != 1 else ''} processed successfully!")
        print("-" * 60)
