"""
catalog.av_record.avro
~~~~~~~~~~~~~~~~~~~
This module contains a class to generate spark tables and structures out of avro record definitions.
"""
from pyspark.sql import types as T

class AvroRecord:
    """AvroRecord class, takes a avro record definition to generate a spark struct/table"""

    avro2sql_types = {
        'boolean': "BOOLEAN",
        'int': "INT",
        'long': "LONG",
        'float': "FLOAT",
        'double': "DOUBLE",
        'bytes': "BINARY",
        'string': "STRING"
    }

    py2sql_types = {
        str : "STRING",
        int : "INT",
        float : "FLOAT"
    }

    avro2spark_types = {
        'boolean': T.BooleanType,
        'int': T.IntegerType,
        'long': T.LongType,
        'float': T.FloatType,
        'double': T.DoubleType,
        'bytes': T.BinaryType,
        'string': T.StringType
    }

    py2spark_types = {
        str : T.StringType,
        int : T.IntegerType,
        float : T.FloatType
    }

    def __init__(self, av_record):
        self.av_record = av_record

    def get_enum_symbol_type(self, types):
        """Return the type of data of the symbols of an enum from an avro type definition"""
        types = set([type(s) for s in types.get('symbols')])
        if len(types) > 1:
            raise ValueError(f"Inconsistent types of symbols in enum {types}")
        _type = types.pop()
        if _type in (str, int, float):
            return _type
        raise ValueError(f"Unsuppurted type in enum {types}")

    def get_sql_field_section(self, field):
        """Produce a SQL definition of a column from an avro field definition"""
        fieldname = field.get('name')
        types = field.get('type','')
        can_be_null = False
        if isinstance(types, list):
            if "null" in types and len(types) == 2:
                can_be_null = True
                types = types[:]
                types.remove("null")
                typename = self.avro2sql_types.get(types[0])
                if typename is None:
                    raise ValueError(f"Type {types[0]} for field {fieldname} is not supported")
            else:
                raise NotImplementedError(f"Multiple types for field {fieldname} not supported")
        elif isinstance(types, str):
            typename = self.avro2sql_types.get(types)
            if typename is None:
                raise ValueError(f"Type {types[0]} for field {fieldname} is not supported")
        elif isinstance(types, dict):
            if types.get('type') == 'enum':
                typename = self.py2sql_types.get(self.get_enum_symbol_type(types))
            else:
                raise NotImplementedError(
                        f"Complex type {types.get('type')} for "
                        "field {fieldname} has not been implemented"
                    )
        else:
            raise NotImplementedError(
                    f"Type {types} for field {fieldname} has not been implemented"
                    )
        return f"\t{fieldname} {typename.upper()} {'NOT NULL' if not can_be_null else ''}"

    def get_sql_fields_section(self):
        """Return the sql definition of the columns for the avro record"""
        fields = []
        for field in self.av_record.get('fields'):
            fields.append(self.get_sql_field_section(field))
        return ",\n".join(fields)

    def get_database_name(self):
        """Return the database name for the table definition from the avro
        record namespace. If there are dots in the namespace it will return
        the characters after the last dot"""
        namespace = self.av_record.get('namespace','')
        if namespace:
            return namespace.split('.')[-1]
        else:
            return None

    def get_table_name(self):
        """Return the name of the sql table from the name of the record"""
        dbname = self.get_database_name()
        if dbname:
            return f"{dbname}.{self.av_record.get('name')}"
        else:
            return self.av_record.get('name')

    def get_sql_comment_section(self):
        """Return the comment for the table out of the 'doc' section of the
        avro record declaration"""
        comment = self.av_record.get('doc')
        if comment is None:
            return ''
        return f"COMMENT '{comment}'"

    def validate_sql_partition_section(self, partition_cols):
        """Raise an error if the picked column for partitioning is not
        in the list of the fields of the avro record"""
        # Validate that the partition_cols are in the fields
        fields_set = set()
        for field in self.av_record.get('fields'):
            fields_set.add(field.get('name'))
        for pcol in partition_cols:
            if pcol not in fields_set:
                raise ValueError(
                        f"Partition column {pcol} is not in "
                        "the fields of the av_record: {','.join(fields_set)} "
                        )
        return f"PARTITIONED BY (\n{','.join(partition_cols)}\n)"

    def get_create_table_sql(self,data_source='DELTA', partition_cols=None, location=None):
        """Return the SQL statement to create the spark table"""
        result = f"CREATE TABLE {self.get_table_name()} (\n{self.get_sql_fields_section()}\n)"
        if data_source:
            result += f"\nUSING {data_source}"
        if partition_cols:
            result += f"\n{self.validate_sql_partition_section(partition_cols)}"
        if location:
            result += f'\nLOCATION "{location}"'
        comment_section = self.get_sql_comment_section()
        if comment_section:
            result += f"\n{comment_section}"
        return result

    def get_spark_struct_field(self, field):
        """Return a spark struct definition out of an avro field definition"""
        fieldname = field.get('name')
        types = field.get('type','')
        can_be_null = False
        if isinstance(types, list):
            if "null" in types and len(types) == 2:
                can_be_null = True
                types = types[:]
                types.remove("null")
                sparktype = self.avro2spark_types.get(types[0])
                if sparktype is None:
                    raise ValueError(f"Type {types[0]} for field {fieldname} is not supported")
            else:
                raise NotImplementedError(f"Multiple types for field {fieldname} not supported")
        elif isinstance(types, str):
            sparktype = self.avro2spark_types.get(types)
            if sparktype is None:
                raise ValueError(f"Type {types[0]} for field {fieldname} is not supported")
        elif isinstance(types, dict):
            if types.get('type') == 'enum':
                sparktype = self.py2spark_types.get(self.get_enum_symbol_type(types))
            else:
                raise NotImplementedError(
                        f"Complex type {types.get('type')}"
                        " for field {fieldname} has not been implemented"
                        )
        else:
            raise NotImplementedError(
                    f"Type {types} for field {fieldname} has not been implemented"
                    )
        return T.StructField(fieldname, sparktype(),nullable=can_be_null)

    def get_spark_struct(self):
        """Return a spark struct definition from the avro record definition"""
        fields = []
        for field in self.av_record.get('fields'):
            fields.append(self.get_spark_struct_field(field))
        return T.StructType(fields)

