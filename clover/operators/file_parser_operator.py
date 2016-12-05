import csv
import logging
from collections import OrderedDict
from tempfile import NamedTemporaryFile
from itertools import groupby

import collections
from os.path import basename, splitext


import simplejson
from airflow import macros
from airflow.hooks import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


logger = logging.getLogger(__name__)

data_type_mapping = {
    'INTEGER': int,
    'BOOLEAN': bool,
    'TEXT': str
}

class FileParser(BaseOperator):
    """
    """

    ui_color = '#3b5998'

    @apply_defaults
    def __init__(
            self,
            postgres_conn_id='clover',
            create=True,
            recreate=True,
            data=None,
            specifications=None,
            debug=False,
            *args, **kwargs
    ):
        self.create = create
        self.recreate = recreate
        self.specifications = specifications
        self.data = data
        self.debug = debug
        # self.pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def execute(self, context):
        table_name = splitext(basename(self.specifications))[0]
        specifications = self.parse_specifications()
        table_schema = self.generate_table_schema(specifications)
        create_table_statement = self.generate_create_table_statement(table_schema,
                                                                      table_name)

        mapped_delineations = self.map_delineations(specifications)
        mapped_data_types = self.map_data_types(specifications)
        # self.create_table(create_table_statement)
        # self.load_transformed_records(specifications, table_name)

    def parse_specifications(self, fieldnames):
        reader = csv.reader(open(self.specifications, "r"))
        next(reader)
        for record in reader:
            yield OrderedDict(zip(fieldnames, record))

    def generate_table_schema(self, specifications):
        table_schema = ','.join(
            ['{column_name} {data_type}'.format(column_name=spec['column_name'],
                                                data_type=spec['data_type']) for spec in specifications])
        return table_schema

    def generate_create_table_statement(self, table_schema, table_name):
        create_table_statement = 'CREATE TABLE {table_name} (' \
                                 '{table_schema}' \
                                 ')'.format(table_name=table_name,
                                            table_schema=table_schema)
        return create_table_statement

    def create_table(self, create_table_statement):
        self.pg_hook.run(sql=create_table_statement, autocommit=True)

    def map_delineations(self, specifications):
        start = 0
        for specification in specifications:
            try:
                width = int(specification['width'])
            except ValueError as error:
                logger.error('Invalid value for width specification')
                raise error
            yield {'start': start, 'end': start + width}
            start += width


    def map_data_types(self, specifications):
        for specification in specifications:
            yield {specification['data_type']: 'p'}

    def split_record(self, record, specifications):
        return [record.strip()[spec['start']:spec['end']] for spec in specifications]

    def transform_data_types(self, record, specifications):
        print([column for column in record])
        return [[data_type_mapping[spec['data_type']](column)
                for column in record] for spec in specifications]

    def load_records(self, table_name, specifications_with_delineations):
        with open(self.data) as table_data:
            for record in table_data:
                mapped_record = self.split_record(record, mapped_widths=None)
                transform_data_types = self.transform_data_types(record)
            rows = self.transform_records(table_data.readlines())
            self.pg_hook.insert_rows(table=table_name,
                                     rows=rows,
                                     target_fields=[spec['column_name'] for spec in specifications])
        pass

parser = FileParser()