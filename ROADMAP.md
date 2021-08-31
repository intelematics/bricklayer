# Bricklayer Roadmap

The goal of the roadmap is to document areas the bricklayer core team is
planning to work on in the future or is currently working on. PRs
targeting these areas are very welcome, but please check first with a
core team member that nobody else is working on the same thing.

**Note:** This doesn’t include everything that the core team will work
on, and everything is subject to change.

- Continue making error messages more useful and informative.

- Use [OpenAPI/Swagger Specification](https://swagger.io/specification/) and 
[Avro Specification](https://avro.apache.org/docs/current/gettingstartedpython.html) 
to define schema and generate notebook with CREATE TABLE scripts and PySpark 
custom StructType templates.

- Support generation of Swagger/Avro definition out of existing table schemas.

- Support comparison of existing databricks schema with Swagger/Avro definitions.

- Help to transition from notebooks to code in modules/packages.

- Export MLFlow artifacts to S3.

- Add markdown rendering to AvroRecord.
