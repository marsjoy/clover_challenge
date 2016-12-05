import datetime
import logging
import unittest
from types import GeneratorType
from operators.file_parser_operator import FileParser
# from airflow.hooks import PostgresHook
from tempfile import NamedTemporaryFile
from collections import OrderedDict
import os


logger = logging.getLogger(__name__)


RAW_SPECIFICATIONS = '"column name",width,datatype\n' \
                     'name,10,TEXT\n' \
                     'valid,1,BOOLEAN\n' \
                     'count,3,INTEGER\n'

RAW_DATA = 'Foonyor   1  1\n' \
           'Barzane   0-12\n' \
           'Quuxitude 1103\n'

RAW_RECORDS = ['Foonyor   1  1\n',
               'Barzane   0-12\n',
               'Quuxitude 1103\n']

PARSED_SPECIFICATIONS = [
            OrderedDict((('column_name', 'name'), ('width', '10'), ('data_type', 'TEXT'))),
            OrderedDict((('column_name', 'valid'), ('width', '1'), ('data_type', 'BOOLEAN'))),
            OrderedDict((('column_name', 'count'), ('width', '3'), ('data_type', 'INTEGER')))]

TABLE_SCHEMA = 'name TEXT,' \
               'valid BOOLEAN,' \
               'count INTEGER'

SPECIFICATIONS_WITH_DELINEATIONS = [
    OrderedDict([('column_name', 'name'),
                 ('width', '10'),
                 ('data_type', 'TEXT'),
                 ('start', 0),
                 ('end', 10)]),
    OrderedDict([('column_name', 'valid'),
                 ('width', '1'),
                 ('data_type', 'BOOLEAN'),
                 ('start', 10),
                 ('end', 11)]),
    OrderedDict([('column_name', 'count'),
                 ('width', '3'),
                 ('data_type', 'INTEGER'),
                 ('start', 11),
                 ('end', 14)])]


class TestFileParserOperator(unittest.TestCase):
    def setUp(self):
        with NamedTemporaryFile(delete=False, mode='w') as temp:
            self.specifications = temp.name
            temp.write(RAW_SPECIFICATIONS)
        with NamedTemporaryFile(delete=False, mode='w') as temp:
            self.data = temp.name
            temp.write(RAW_DATA)
        self.parser = FileParser(specifications=self.specifications,
                                 DATA=self.data,
                                 recreate=True,
                                 create=True,
                                 fieldnames=['column_name', 'width', 'data_type'])

    def tearDown(self):
        os.remove(self.data)
        os.remove(self.specifications)

    def test_init(self):
        self.assertTrue(self.parser.recreate)
        self.assertTrue(self.parser.create)
        # self.assertTrue(isinstance(self.parser.pg_hook, PostgresHook))

    def test_parse_specifications(self):
        expected = [
            OrderedDict([('column_name', 'name'), ('width', '10'), ('data_type', 'TEXT')]),
            OrderedDict([('column_name', 'valid'), ('width', '1'), ('data_type', 'BOOLEAN')]),
            OrderedDict([('column_name', 'count'), ('width', '3'), ('data_type', 'INTEGER')])]
        specifications = self.parser.parse_specifications(['column_name', 'width', 'data_type'])
        self.assertTrue(isinstance(specifications, GeneratorType))
        self.assertEquals(expected, list(specifications))

    def test_generate_table_schema(self):
        expected = 'name TEXT,valid BOOLEAN,count INTEGER'
        table_schema = self.parser.generate_table_schema(PARSED_SPECIFICATIONS)
        self.assertEquals(expected, table_schema)

    def test_generate_create_table_statement(self):
        expected = 'CREATE TABLE test_table (name TEXT,valid BOOLEAN,count INTEGER)'
        create_table_statement = self.parser.generate_create_table_statement(table_schema=TABLE_SCHEMA,
                                                                             table_name='test_table')
        self.assertEquals(expected, create_table_statement)

    def test_map_delineations(self):
        expected = [{'end': 10, 'start': 0},
                    {'end': 11, 'start': 10},
                    {'end': 14, 'start': 11}]
        specifications_with_delineations = self.parser.map_delineations(specifications=PARSED_SPECIFICATIONS)
        self.assertEquals(expected, list(specifications_with_delineations))

    def test_split_record(self):
        expected = ['Foonyor   ', '1', '  1']
        transformed_record = self.parser.split_record('Foonyor   1  1\n',
                                                      SPECIFICATIONS_WITH_DELINEATIONS)
        self.assertEquals(expected, transformed_record)

    def test_transform_data_types(self):
        expected = []
        transformed_record = self.parser.transform_data_types(['Foonyor   ', '1', '  1'],
                                                              SPECIFICATIONS_WITH_DELINEATIONS)
        self.assertEquals(expected, transformed_record)
