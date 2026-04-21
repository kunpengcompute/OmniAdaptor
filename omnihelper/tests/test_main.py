import unittest

from omnihelper.main import LogParser


class LogParserTestCase(unittest.TestCase):
    def test_get_column_type(self):
        table_schema = {
            "test_db.table1": [
                {
                    "column_name":"c1",
                    "data_type":"int"
                }
            ]
        }
        physical_plan = ("(6) LogicalRelation\n"
                         "Arguments: orc, [c_chain#0, user_id#1], `test_db`.`table1`, "
                         "org.apache.hadoop.hive.ql.io.orc.OrcSerde, false")
        column_type = LogParser.get_column_type(table_schema, physical_plan)
        self.assertEqual(column_type.get("c1"), "int")
