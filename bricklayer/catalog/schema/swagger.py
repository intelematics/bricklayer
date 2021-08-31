"""Module to generate spark tables ddl, spark structures and markup out of swagger definitions."""

from pyspark.sql import types as T
import yaml

_swagger2sql_types = {
    'boolean': 'BOOLEAN',
    'number': 'DOUBLE',
    'geography': 'STRING',
    'integer': 'INT',
    'int64': 'BIGINT',
    'date': 'DATE',
    'date-time': 'TIMESTAMP',
    'date-time-zoneless': 'TIMESTAMP',
    'long': 'LONG',
    'float': 'FLOAT',
    'double': 'DOUBLE',
    'bytes': 'BINARY',
    'string': 'STRING'
}
_swagger2spark_types = {
    'boolean': T.BooleanType,
    'number': T.DoubleType,
    'geography': T.StringType,
    'integer': T.IntegerType,
    'int64': T.IntegerType,
    'date': T.DateType,
    'date-time': T.TimestampType,
    'date-time-zoneless': T.TimestampType,
    'long': T.LongType,
    'float': T.FloatType,
    'double': T.DoubleType,
    'bytes': T.BinaryType,
    'string': T.StringType
}

class SwaggerRecord:

    indent = ' ' * 2

    def __init__(self, swaggerString: dict):
        """Takes input swagger in form of string"""
        self.swagger = yaml.safe_load(swaggerString)

    def get_tables(self):
        """Yields all the tables from the swagger spec"""
        for name in sorted(self.swagger['components']['schemas'].keys()):
            tbl = self.swagger['components']['schemas'][name]
            if 'x-data-asset-table' in tbl:
                yield tbl

    def get_create_table_sql(self):
        """renders sql ddl for every table specied in swagger"""
        for table in self.get_tables():
            yield self.render_swagger_table_ddl(Table(table))

    def get_spark_struct(self):
        """renders sql ddl for every table specied in swagger"""
        for table in self.get_tables():
            yield self.render_swagger_table_spark_struct(Table(table))

    def get_markup(self):
        """renders sql ddl for every table specied in swagger"""
        for table in self.get_tables():
            yield self.render_swagger_table_markup(Table(table))

    def render_swagger_table_ddl(self, table):
        """Rendering ddl for each table"""
        def render_fields(table):
            """Renders fields for query, e.g `colname` TYPE COMMENT 'blah'"""
            return [
                ',\n'.join([
                    f"{self.indent}{f.field_name} {_swagger2sql_types[f.type]}"
                    for f in table.fields
                ])
            ]
        if len(table.location) == 0:
            location = f'/mnt/data_asset/{table.schema_name}.{table.table_name}/version={table.version}'
        else:
            location = table.location
        fields = render_fields(table)
        partition_sql = []
        if len(table.partition_keys) > 0:
            partition_sql = [
                f"PARTITIONED BY (",
                ',\n'.join([
                    f"{self.indent}{k}"
                    for k in table.partition_keys
                ]),
                f")"
            ]
        unique_keys = ', '.join([
            f'"{t}"'
            for t in table.unique_keys
        ])
        sql = '\n'.join([
            f"CREATE TABLE {table.schema_name}.{table.table_name}_version_{table.version} (",
            *fields,
            f")",
            f"USING DELTA ",
            *partition_sql,
            f"LOCATION '{location}'",
            f"TBLPROPERTIES ('unique_keys' = '[{unique_keys}]')"
            f"",
            f"",
        ])
        return sql

    def render_swagger_table_spark_struct(self, table):
        """Return a spark struct definition from the avro record definition"""
        fields = [
            T.StructField(
                f.field_name,
                _swagger2spark_types[f.type](),
                f.field_name not in table.unique_keys and f.field_name not in table.partition_keys
            )
            for f in table.fields
        ]
        return T.StructType(fields)

    def render_swagger_table_markup(self, table):
        """Rendering markup for each table"""
        def get_obsoleted_by_render(table):
            if table.obsoleted_by == None:
                return []
            else:
                return ['## Obsoleted by\n- ' + f'\n- '.join(table.obsoleted_by) + '\n']
        def get_partition_keys_render(table):
            if len(table.partition_keys) == 0:
                return 'There are no partition keys for this table'
            else:
                return '- ' + f'\n- '.join(table.partition_keys)
        def get_unique_keys_render(table):
            if len(table.unique_keys) == 0:
                return 'There are no unique keys for this table'
            else:
                return '- ' + f'\n- '.join(table.unique_keys)
        def get_source_urls_render(table):
            if len(table.source_urls) == 0:
                return 'There are no source URLs for this table'
            else:
                return '\n'.join([f'- {url}' for url in table.source_urls])
        def get_changelog_render(table):
            if len(table.changelog) == 0:
                return 'There are no changelog entries for this table'
            else:
                entries = []
                for change in table.changelog:
                    entry = (
                        "- **Version {version}**\n\n"
                        "  {description}\n"
                    ).format(**change)
                    entries.append(entry)
                return '\n'.join(entries)
        def get_properties_for_table(table):
            fieldLines = [
                f'| {f.field_name} '
                    f'| {render_property_type(f)} '
                    f'| {f.description} '
                    f'| {f.example} |'
                for f in table.fields
            ]
            return f'\n'.join(fieldLines)
        def render_property_type(field):
            mysql_type = field.raw_spec.get('x-mysql-type')
            if mysql_type == 'text':
                return 'longtext'
            elif mysql_type and mysql_type not in ['datetime', 'geography']:
                return mysql_type
            if field.type == 'string':
                if field.enum:
                    s = ",".join([f"'{value}'" for value in sorted(field.enum)])
                    return f'ENUM({s})'
                elif field.maxLength:
                    return f'varchar({field.maxLength})'
                else:
                    return 'varchar(64)'
            else:
                return _swagger2sql_types.get(field.type, field.type)
        def get_dependencies_render(table):
            if len(table.dependencies) == 0:
                return 'There are no dependencies for this table'
            else:
                return '\n'.join([f'- `{dep}`' for dep in table.dependencies])
        markup = '\n'.join([
            f'# {table.schema_name}.{table.table_name}',
            f'{table.description}\n',
            *get_obsoleted_by_render(table),
            f'## Partition Keys',
            f'{get_partition_keys_render(table)}\n',
            f'## Unique Keys',
            f'{get_unique_keys_render(table)}\n',
            f'',
            f'| **Property Name** | **Property Type** | **Property Comment** | **Property Example** |',
            f'| ---------- | --------- | ----------- | --------- |',
            f'{get_properties_for_table(table)}\n',
            f''
            f'## Dependencies',
            f'{get_dependencies_render(table)}\n'
            f'## Sources',
            f'{get_source_urls_render(table)}\n',
            f'## Changelog',
            f'',
            f'{get_changelog_render(table)}\n'
        ])
        return markup

class TableField:
    def __init__(self, field_name, field_spec):
        self.raw_spec = field_spec # use only for target-specialized properties

        self.field_name = field_name
        self.description = field_spec.get('description', '').replace('\n', ' ')
        self.example = str(field_spec.get('example', '')).replace('\n', ' ')

        t = field_spec['type'] # mandatory
        f = field_spec.get('format', '')
        if t in ['boolean', 'number', 'geography']:
            self.type = t
        elif t == 'integer':
            if f == 'int64':
                self.type = f
            else:
                self.type = t
        elif t == 'string':
            if f == 'datetime':
                self.type = 'date-time'
            elif f in ['date', 'date-time', 'date-time-zoneless']:
                self.type = f
            else:
                self.type = t
                self.maxLength = field_spec.get('maxLength')
                self.enum = field_spec.get('enum')
        elif t in _swagger2sql_types:
            self.type = t
        else:
            raise TypeError(f'{field_name}: {field_spec}')

class Table:
    def __init__(self, tbl_spec):
        self.raw_spec = tbl_spec # use only for target-specialized properties

        self.schema_name = tbl_spec['x-data-asset-schema'] # mandatory
        self.table_name = tbl_spec['x-data-asset-table'] # mandatory
        self.version = tbl_spec['x-data-asset-version'] # mandatory
        self.location = tbl_spec.get('x-data-asset-location', []) or []
        self.description = tbl_spec.get('description', '').replace('\n', ' ')
        self.partition_keys = tbl_spec.get('x-data-asset-partition-keys', []) or []
        self.unique_keys = tbl_spec.get('x-data-asset-unique-keys', []) or []
        self.relationships = tbl_spec.get('x-data-asset-relationships', []) or []
        self.dependencies = tbl_spec.get('x-data-asset-dependencies', []) or []
        self.source_urls = tbl_spec.get('x-data-asset-source-urls', []) or []
        self.changelog = tbl_spec.get('x-data-asset-changelog', []) or []
        self.obsoleted_by = tbl_spec.get('x-data-asset-obsoleted-by') # None means not obsolete

        def get_table_properties(table):
            if 'allOf' in table:
                for definition in table['allOf']:
                    yield from get_table_properties(definition)
            elif 'properties' in table.keys():
                for key, value in table['properties'].items():
                    yield key, value

        self.fields = [TableField(key, value) for key, value in get_table_properties(tbl_spec)]
