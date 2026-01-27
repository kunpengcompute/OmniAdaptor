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


import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter


class ExcelWriterWithStyle:
    """带样式的Excel写入器"""

    def __init__(self):
        # 定义列标题
        self.main_titles = [
            'ApplicationID+SQL ID',
            'SQL Hash',
            'Omni不支持的算子',
            None, None, None, None, None,  # 3-7列的占位
            'Omni不支持的表达式/内置函数',
            None, None,  # 8-10列的占位
            'Spark版本',
            '异常信息/备注'
        ]

        self.sub_titles = [
            'ApplicationID+SQL ID', 'SQL Hash',
            '名称', 'Input', 'Output', '出现频次', '运行时间', 'Output rows',
            '名称', 'Input', '出现频次',
            'Spark版本', '异常信息/备注'
        ]

        # 定义列宽
        self.column_widths = [35, 10, 20, 35, 35, 10, 35, 15, 20, 35, 10, 15, 30]

        # 定义样式
        self.center_alignment = Alignment(horizontal='center', vertical='center')
        self.bold_font = Font(bold=True)
        self.header_fill = PatternFill(start_color="EBEFF6", end_color="EBEFF6", fill_type="solid")
        self.thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

    def write_to_excel(self, df, output_excel_path, sheet_name='Sheet1'):
        """
        将DataFrame写入Excel并应用样式

        Parameters:
        -----------
        df : pandas.DataFrame
            要写入的数据
        output_excel_path : str
            输出Excel文件路径
        sheet_name : str, optional
            工作表名称，默认为'Sheet1'
        """
        print("Start writing to excel...")

        try:
            with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
                # 先写入数据
                df.to_excel(writer, index=False, header=False, startrow=2)

                # 获取工作簿和工作表
                workbook = writer.book
                worksheet = writer.sheets[sheet_name]

                # 设置标题
                self._write_headers(worksheet)

                # 合并单元格
                self._merge_cells(worksheet, df)

                # 应用样式
                self._apply_styles(worksheet, df)

                # 设置列宽
                self._set_column_widths(worksheet)

                # 合并相同值的单元格
                self._merge_duplicate_cells(worksheet, df)

            print(f"[SUCCESS] Analysis report has been saved to: {output_excel_path}")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to write Excel file: {e}")
            return False

    def _write_headers(self, worksheet):
        """写入表头"""
        # 第一行主标题
        for col_idx, title in enumerate(self.main_titles, 1):
            if title:  # 只写入有值的标题
                worksheet.cell(row=1, column=col_idx, value=title)

        # 第二行子标题
        for col_idx, title in enumerate(self.sub_titles, 1):
            worksheet.cell(row=2, column=col_idx, value=title)

    def _merge_cells(self, worksheet, df):
        """合并单元格"""
        # 第1列：第1-2行合并
        worksheet.merge_cells(start_row=1, start_column=1, end_row=2, end_column=1)
        # 第2列：第1-2行合并
        worksheet.merge_cells(start_row=1, start_column=2, end_row=2, end_column=2)
        # 第3-8列：第1行合并（Omni不支持的算子）
        worksheet.merge_cells(start_row=1, start_column=3, end_row=1, end_column=8)
        # 第9-11列：第1行合并（Omni不支持的表达式/内置函数）
        worksheet.merge_cells(start_row=1, start_column=9, end_row=1, end_column=11)
        # 第12列：第1-2行合并
        worksheet.merge_cells(start_row=1, start_column=12, end_row=2, end_column=12)
        # 第13列：第1-2行合并
        worksheet.merge_cells(start_row=1, start_column=13, end_row=2, end_column=13)

    def _apply_styles(self, worksheet, df):
        """应用样式"""
        total_rows = len(df) + 2  # 数据行数 + 标题行
        total_cols = len(self.sub_titles)

        # 为所有单元格应用样式
        for row in range(1, total_rows + 1):
            for col in range(1, total_cols + 1):
                cell = worksheet.cell(row=row, column=col)

                # 所有单元格应用边框
                cell.border = self.thin_border

                # 前两行（标题行）应用特殊样式
                if row <= 2:
                    cell.font = self.bold_font
                    cell.fill = self.header_fill
                    cell.alignment = self.center_alignment
                else:
                    # 数据行应用居中对齐
                    cell.alignment = self.center_alignment

    def _set_column_widths(self, worksheet):
        """设置列宽"""
        for col_idx, width in enumerate(self.column_widths, 1):
            column_letter = get_column_letter(col_idx)
            worksheet.column_dimensions[column_letter].width = width

    def _merge_duplicate_cells(self, worksheet, df):
        """合并相同值的单元格"""
        start_data_row = 3
        current_start_row = start_data_row
        total_rows = len(df) + 2

        # 遍历所有数据行
        for current_row in range(start_data_row, total_rows + 1):
            current_col1_value = worksheet.cell(row=current_row, column=1).value
            current_col2_value = worksheet.cell(row=current_row, column=2).value

            # 获取下一行的值（如果存在）
            if current_row < total_rows:
                next_col1_value = worksheet.cell(row=current_row + 1, column=1).value
                next_col2_value = worksheet.cell(row=current_row + 1, column=2).value
            else:
                next_col1_value = None
                next_col2_value = None

            # 检查当前行是否与下一行的第1、2列值相同
            same_values = (current_col1_value == next_col1_value and
                           current_col2_value == next_col2_value)

            # 如果值不相同或者已经到最后一行，则合并之前相同的行
            if not same_values:
                if current_start_row < current_row:
                    # 合并第1列
                    worksheet.merge_cells(
                        start_row=current_start_row,
                        start_column=1,
                        end_row=current_row,
                        end_column=1
                    )
                    # 合并第2列
                    worksheet.merge_cells(
                        start_row=current_start_row,
                        start_column=2,
                        end_row=current_row,
                        end_column=2
                    )

                    # 设置合并后的单元格样式
                    for col in [1, 2]:
                        merged_cell = worksheet.cell(row=current_start_row, column=col)
                        merged_cell.alignment = self.center_alignment
                        merged_cell.border = self.thin_border

                # 更新起始行为下一行
                current_start_row = current_row + 1