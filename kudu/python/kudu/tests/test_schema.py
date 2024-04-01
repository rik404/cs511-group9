#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import division

from kudu.compat import CompatUnitTest
from kudu.errors import KuduInvalidArgument
import kudu

from kudu.schema import Schema

class TestSchema(CompatUnitTest):

    def setUp(self):
        self.columns = [('one', 'int32', False),
                        ('two', 'int8', False),
                        ('three', 'double', True),
                        ('four', 'string', False)]

        self.primary_keys = ['one', 'two']

        self.builder = kudu.schema_builder()
        for name, typename, nullable in self.columns:
            self.builder.add_column(name, typename, nullable=nullable)

        self.builder.set_primary_keys(self.primary_keys)
        self.schema = self.builder.build()

    def test_repr(self):
        result = repr(self.schema)
        for name, _, _ in self.columns:
            assert name in result

        assert 'PRIMARY KEY (one, two)' in result

    def test_schema_length(self):
        assert len(self.schema) == 4

    def test_names(self):
        assert self.schema.names == ['one', 'two', 'three', 'four']

    def test_primary_keys(self):
        assert self.schema.primary_key_indices() == [0, 1]
        assert self.schema.primary_keys() == ['one', 'two']

    def test_getitem_boundschecking(self):
        idx = 4
        error_msg = 'Column index {0} is not in range'.format(idx)
        with self.assertRaisesRegex(IndexError, error_msg):
            self.schema[idx]

    def test_getitem_wraparound(self):
        # wraparound
        result = self.schema[-1]
        expected = self.schema[3]

        assert result.equals(expected)

    def test_getitem_string(self):
        result = self.schema['three']
        expected = self.schema[2]

        assert result.equals(expected)

        error_msg = 'not_found'
        with self.assertRaisesRegex(KeyError, error_msg):
            self.schema['not_found']

    def test_schema_equals(self):
        assert self.schema.equals(self.schema)

        builder = kudu.schema_builder()
        builder.add_column('key', 'int64', nullable=False, primary_key=True)
        schema = builder.build()

        assert not self.schema.equals(schema)

    def test_column_equals(self):
        assert not self.schema[0].equals(self.schema[1])

    def test_type(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('int32')
         .primary_key()
         .block_size(1048576)
         .nullable(False))
        schema = builder.build()

        tp = schema[0].type
        assert tp.name == 'int32'
        assert tp.type == kudu.schema.INT32

    def test_compression(self):
        builder = kudu.schema_builder()
        builder.add_column('key', 'int64', nullable=False)

        foo = builder.add_column('foo', 'string').compression('lz4')
        assert foo is not None

        bar = builder.add_column('bar', 'string')
        bar.compression(kudu.COMPRESSION_ZLIB)

        compression = 'unknown'
        error_msg = 'Invalid compression type: {0}'.format(compression)
        with self.assertRaisesRegex(ValueError, error_msg):
            bar = builder.add_column('qux', 'string', compression=compression)

        builder.set_primary_keys(['key'])
        builder.build()

        # TODO; The C++ client does not give us an API to see the storage
        # attributes of a column

    def test_encoding(self):
        builder = kudu.schema_builder()
        builder.add_column('key', 'int64', nullable=False)

        available_encodings = ['auto', 'plain', 'prefix', 'bitshuffle',
                               'rle', 'dict', kudu.ENCODING_DICT]
        for enc in available_encodings:
            foo = builder.add_column('foo_%s' % enc, 'string').encoding(enc)
            assert foo is not None
            del foo

        bar = builder.add_column('bar', 'string')
        bar.encoding(kudu.ENCODING_PLAIN)

        error_msg = 'Invalid encoding type'
        with self.assertRaisesRegex(ValueError, error_msg):
            builder.add_column('qux', 'string', encoding='unknown')

        builder.set_primary_keys(['key'])
        builder.build()
        # TODO(wesm): The C++ client does not give us an API to see the storage
        # attributes of a column

    def test_decimal(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('decimal')
         .primary_key()
         .nullable(False)
         .precision(9)
         .scale(2))
        schema = builder.build()

        column = schema[0]
        tp = column.type
        assert tp.name == 'decimal'
        assert tp.type == kudu.schema.DECIMAL
        ta = column.type_attributes
        assert ta.precision == 9
        assert ta.scale == 2

    def test_decimal_without_precision(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('decimal')
         .primary_key()
         .nullable(False))

        error_msg = 'no precision provided for decimal column: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_precision_on_non_decimal_column(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('int32')
         .primary_key()
         .nullable(False)
         .precision(9)
         .scale(2))

        error_msg = 'precision is not valid on a 2 column: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_date(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('date')
         .primary_key()
         .nullable(False))
        schema = builder.build()

        column = schema[0]
        tp = column.type
        assert tp.name == 'date'
        assert tp.type == kudu.schema.DATE

    def test_varchar(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('varchar')
         .primary_key()
         .nullable(False)
         .length(10))
        schema = builder.build()

        column = schema[0]
        tp = column.type
        assert tp.name == 'varchar'
        assert tp.type == kudu.schema.VARCHAR
        ta = column.type_attributes
        assert ta.length == 10

    def test_varchar_without_length(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('varchar')
         .primary_key()
         .nullable(False))

        error_msg = 'no length provided for VARCHAR column: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_varchar_invalid_length(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('varchar')
         .primary_key()
         .length(0)
         .nullable(False))

        error_msg = 'length must be between 1 and 65535: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_length_on_non_varchar_column(self):
        builder = kudu.schema_builder()
        (builder.add_column('key')
         .type('decimal')
         .primary_key()
         .nullable(False)
         .length(10))

        error_msg = 'no precision provided for decimal column: key'
        with self.assertRaisesRegex(kudu.KuduInvalidArgument, error_msg):
            builder.build()

    def test_unsupported_col_spec_methods_for_create_table(self):
        builder = kudu.schema_builder()
        builder.add_column('test', 'int64').rename('test')
        error_msg = 'cannot rename a column during CreateTable: test'
        with self.assertRaisesRegex(kudu.KuduNotSupported, error_msg):
            builder.build()

        builder.add_column('test', 'int64').remove_default()
        error_msg = 'cannot rename a column during CreateTable: test'
        with self.assertRaisesRegex(kudu.KuduNotSupported, error_msg):
            builder.build()

    def test_set_column_spec_pk(self):
        builder = kudu.schema_builder()
        key = (builder.add_column('key', 'int64', nullable=False)
               .primary_key())
        assert key is not None
        schema = builder.build()
        assert 'key' in schema.primary_keys()

        builder = kudu.schema_builder()
        key = (builder.add_column('key', 'int64', nullable=False,
                                  primary_key=True))
        schema = builder.build()
        assert 'key' in schema.primary_keys()

    def test_partition_schema(self):
        pass

    def test_nullable_not_null(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64', nullable=False)
         .primary_key())

        builder.add_column('data1', 'double').nullable(True)
        builder.add_column('data2', 'double').nullable(False)
        builder.add_column('data3', 'double', nullable=True)
        builder.add_column('data4', 'double', nullable=False)

        schema = builder.build()

        assert not schema[0].nullable
        assert schema[1].nullable
        assert not schema[2].nullable

        assert schema[3].nullable
        assert not schema[4].nullable

    def test_mutable_immutable(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64', nullable=False)
         .primary_key())

        builder.add_column('data1', 'double').mutable(True)
        builder.add_column('data2', 'double').mutable(False)

        schema = builder.build()

        assert schema[0].mutable
        assert schema[1].mutable
        assert not schema[2].mutable

    def test_column_comment(self):
        comment = "test_comment"
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64', nullable=False)
         .primary_key()
         .comment(comment))

        builder.add_column('data1', 'double').nullable(True)
        schema = builder.build()
        assert isinstance(schema[0].comment, str)
        assert len(schema[0].comment) > 0
        assert schema[0].comment == comment
        assert isinstance(schema[1].comment, str)
        assert len(schema[1].comment) == 0

    def test_auto_incrementing_column_name(self):
        name = Schema.get_auto_incrementing_column_name()
        assert isinstance(name, str)
        assert len(name) > 0

    def test_non_unique_primary_key(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .non_unique_primary_key())
        builder.add_column('data1', 'double')
        schema = builder.build()
        assert len(schema) == 3
        assert len(schema.primary_keys()) == 2
        assert Schema.get_auto_incrementing_column_name() in schema.primary_keys()

    def test_set_non_unique_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False))
        builder.add_column('data1', 'double')
        builder.set_non_unique_primary_keys(['key'])
        schema = builder.build()
        assert len(schema) == 3
        assert len(schema.primary_keys()) == 2
        assert Schema.get_auto_incrementing_column_name() in schema.primary_keys()

    def test_set_non_unique_primary_keys_wrong_order(self):
        builder = kudu.schema_builder()
        builder.add_column('key1', 'int64').nullable(False)
        builder.add_column('key2', 'double').nullable(False)
        builder.set_non_unique_primary_keys(['key2', 'key1'])
        error_msg = 'primary key columns must be listed first in the schema: key'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            schema = builder.build()

    def test_set_non_unique_primary_keys_not_first(self):
        builder = kudu.schema_builder()
        builder.add_column('data1', 'double')
        (builder.add_column('key', 'int64')
         .nullable(False))
        builder.set_non_unique_primary_keys(['key'])
        error_msg = 'primary key columns must be listed first in the schema: key'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            schema = builder.build()

    def test_set_non_unique_primary_keys_same_name_twice(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False))
        builder.add_column('data1', 'double')
        builder.set_non_unique_primary_keys(['key', 'key'])
        error_msg = 'primary key columns must be listed first in the schema: key'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            schema = builder.build()

    def test_unique_and_non_unique_primary_key_on_same_column(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .primary_key()
         .non_unique_primary_key())
        builder.add_column('data1', 'double')
        schema = builder.build()
        assert len(schema) == 3
        assert len(schema.primary_keys()) == 2
        assert Schema.get_auto_incrementing_column_name() in schema.primary_keys()

    def test_non_unique_and_unique_primary_key_on_same_column(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .non_unique_primary_key()
         .primary_key())
        builder.add_column('data1', 'double')
        schema = builder.build()
        assert len(schema) == 2
        assert len(schema.primary_keys()) == 1
        assert Schema.get_auto_incrementing_column_name() not in schema.primary_keys()

    def test_non_unique_primary_key_not_first(self):
        builder = kudu.schema_builder()
        builder.add_column('data1', 'int64')
        (builder.add_column('key', 'double')
         .nullable(False)
         .non_unique_primary_key())
        error_msg = 'primary key column must be the first column'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_unique_and_non_unique_primary_key_on_different_cols(self):
        builder = kudu.schema_builder()
        (builder.add_column('key1', 'double')
         .nullable(False)
         .primary_key())
        (builder.add_column('key2', 'double')
         .nullable(False)
         .non_unique_primary_key())
        error_msg = 'multiple columns specified for primary key: key1, key2'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_non_unique_and_unique_primary_key_on_different_cols(self):
        builder = kudu.schema_builder()
        (builder.add_column('key1', 'double')
         .nullable(False)
         .non_unique_primary_key())
        (builder.add_column('key2', 'double')
         .nullable(False)
         .primary_key())
        error_msg = 'multiple columns specified for primary key: key1, key2'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_multiple_non_unique_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key1', 'double')
         .nullable(False)
         .non_unique_primary_key())
        (builder.add_column('key2', 'double')
         .nullable(False)
         .non_unique_primary_key())
        error_msg = 'multiple columns specified for primary key: key1, key2'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_non_unique_primary_key_and_set_non_unique_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .non_unique_primary_key())
        builder.add_column('data1', 'double')
        builder.set_non_unique_primary_keys(['key'])
        error_msg = ('primary key specified by both SetNonUniquePrimaryKey\(\)'
                     ' and on a specific column: key')
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_primary_key_and_set_non_unique_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .primary_key())
        builder.add_column('data1', 'double')
        builder.set_non_unique_primary_keys(['key'])
        error_msg = ('primary key specified by both SetNonUniquePrimaryKey\(\)'
                     ' and on a specific column: key')
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_primary_key_and_set_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .primary_key())
        builder.add_column('data1', 'double')
        builder.set_primary_keys(['key'])
        error_msg = ('primary key specified by both SetPrimaryKey\(\)'
                     ' and on a specific column: key')
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_non_unique_primary_key_and_set_primary_keys(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .non_unique_primary_key())
        builder.add_column('data1', 'double')
        builder.set_primary_keys(['key'])
        error_msg = ('primary key specified by both SetPrimaryKey\(\)'
                     ' and on a specific column: key')
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_set_non_unique_and_set_unique_primary_key(self):
        builder = kudu.schema_builder()
        builder.add_column('key1', 'int64').nullable(False)
        builder.add_column('key2', 'double').nullable(False)
        builder.set_non_unique_primary_keys(['key1', 'key2'])
        builder.set_primary_keys(['key1', 'key2'])
        schema = builder.build()
        assert len(schema) == 2
        assert len(schema.primary_keys()) == 2
        assert Schema.get_auto_incrementing_column_name() not in schema.primary_keys()

    def test_set_unique_and_set_non_unique_primary_key(self):
        builder = kudu.schema_builder()
        builder.add_column('key1', 'int64').nullable(False)
        builder.add_column('key2', 'double').nullable(False)
        builder.set_primary_keys(['key1', 'key2'])
        builder.set_non_unique_primary_keys(['key1', 'key2'])
        schema = builder.build()
        assert len(schema) == 3
        assert len(schema.primary_keys()) == 3
        assert Schema.get_auto_incrementing_column_name() in schema.primary_keys()

    def test_reserved_column_name(self):
        builder = kudu.schema_builder()
        (builder.add_column('key', 'int64')
         .nullable(False)
         .primary_key())
        builder.add_column(Schema.get_auto_incrementing_column_name(), 'double')
        error_msg = 'auto_incrementing_id is a reserved column name'
        with self.assertRaisesRegex(KuduInvalidArgument, error_msg):
            builder.build()

    def test_default_value(self):
        pass

    def test_column_schema_repr(self):
        result = repr(self.schema[0])
        expected = 'ColumnSchema(name=one, type=int32, nullable=False)'
        self.assertEqual(result, expected)
