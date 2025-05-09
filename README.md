# DAS Data Files

[![License](https://img.shields.io/:license-BSL%201.1-blue.svg)](/licenses/BSL.txt)

[Data Access Service](https://github.com/raw-labs/protocol-das)
This is a Scala/Spark plugin that reads data from multiple file systems (Local, S3, GitHub)
in various formats (CSV, JSON, Parquet, XML)

This is the list of DAS types released in this DAS server:

* DAS CSV
* DAS JSON
* DAS XML
* DAS Parquet

## Global Options

| Config Key       | Description                                                                                        | Example                               |
|------------------|----------------------------------------------------------------------------------------------------|---------------------------------------|
| paths            | The number of paths to define.                                                                     | paths '2'                             |
| path{i}_url      | Full URL or wildcard pointing to the file/folder. Required for each path index i.                  | path0_url 's3://bucket/path/file.csv' |
| path{i}_name     | Human-friendly table name override for path i. If omitted, the name is derived from the file name. | path0_name 'my_Table'                 |
| path{i}_*        | Specific option for path`i` e.g. for csv `path0_header`                                            | path0_header 'true'                   |
| aws_access_key   | The aws access key for s3 buckets urls (optional).                                                 | aws_access_key 'my-key'               |
| aws_secret_key   | The aws secret key for s3 buckets urls (optional). Has to be defined together with aws_secret_key. | aws_secret_key 'my-secret'            |
| aws_region       | The aws region (optional).                                                                         | aws_region 'eu-west-1'                |
| github_api_token | The github api-token for github urls (optional).                                                   | github_api_token 'token'              |

For example:

```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 'csv',
  
  -- github credentials (optional)
  github_api_token 'token'
  
  paths '2',
 
  path0_url 'github://owner/repo/branch/path1/data.csv',
   
  path1_url 'github://owner/repo/branch/path2/*.csv'
);
```

## URL Schemes & Wildcard Resolution

The Data Files DAS Plugin supports several URI “schemes” to indicate the filesystem where the data resides.
You can also use wildcard patterns (e.g. *.csv) to match multiple files at once.

### Supported URL Schemes

**Local Files**

* `file://` (e.g. `file:///home/user/data.csv`)
* No scheme: If the URL has no scheme, it is treated as local (e.g. /home/user/data.csv).
* **Note:** Local file access is only permitted if raw.das.data-files.allow-local-files is true. Otherwise, a
  DASSdkInvalidArgumentException will be thrown.

**Amazon S3**

* `s3://bucket/key`
* The plugin uses the AWS SDK to list and download objects. If you do not provide aws_access_key and aws_secret_key, the
  code attempts anonymous credentials (which usually only work for public buckets).

**GitHub**

* `github://owner/repo/branch/path/to/file_or_folder`
* Private repos require a github_api_token.

### Wildcard Resolution

URLs can contain wildcard patterns such as *.csv or *.json.
This allows you to create multiple “tables” for a single path definition in your config. For example:

```sql
path0_url 's3://my-bucket/data/*.csv'
```

Would match all CSV files in the data/ folder.

Wildcard resolution is single-level, filtering within a “directory” or prefix.

1. Internally, the plugin looks for special glob symbols (*, ?) in the final segment of the path.
    * If no glob symbols are present, the plugin treats the path as a single file or “directory” to be listed.
    * If there is a wildcard (e.g. *.csv, file_?.csv), the plugin separates the “prefix”
      (folder path) from the pattern (filename glob).
1. Multiple Results
    * If the wildcard matches multiple files, each matched file gets its own table.
      The plugin tries to derive a name from the filename (or use the user-supplied path{i}_name, with _suffix
      appended).
    * If you have paths=1 but path0_url matches 10 files, you end up with 10 “tables” in your DAS environment.
    * If path0_name=my_table, the tables will be named my_table_{filename1}, my_table_{filename2}, etc.

### Pattern limitations

The project’s wildcard resolution is relatively basic:

* `*` matches zero or more characters in a single path segment.
* `?` matches exactly one character in a single path segment.
* Bracket expressions like [abc] or extended patterns like {foo,bar} are not supported.

For recursive listing across subdirectories Provide additional path definitions (e.g. path1_url, path2_url) for
different folders,

## DAS CSV "csv" Options

| Config Key                            | Description                                                                      | Default Value   | Mandatory |
|---------------------------------------|----------------------------------------------------------------------------------|-----------------|-----------|
| path{i}_header                        | Whether the first line is a header row.                                          | true            | No        |
| path{i}_delimiter                     | Field delimiter character                                                        | ,               | No        |
| path{i}_quote                         | Quote character for enclosed fields.                                             | "               | No        |
| path{i}_escape                        | Escape character for quotes inside quoted fields.                                | \               | No        |
| path{i}_multiline                     | Whether a single record can span multiple lines .                                | false'          | No        |
| path{i}_mode                          | The mode for parsing CSV files, one of PERMISSIVE, DROPMALFORMED, FAILFAST.      | PERMISSIVE      | No        |
| path{i}_date_format                   | Custom date format for parsing date fields, e.g. yyyy-MM-d.                      |                 | No        |
| path{i}_timestamp_format              | Custom timestamp format, e.g. yyyy-MM-dd'T'HH:mm:ss .                            |                 | No        |
| path{i}_ignore_leading_white_space    | Ignore leading whitespaces in CSV fields.                                        | false           | No        |
| path{i}_ignore_trailing_whiteSpace    | Ignore leading whitespaces in CSV fields.                                        | false           | No        |
| path{i}_null_value                    | The string representation of a null value.                                       | empty string "" | No        |
| path{i}_nan_value                     | The string representation of a NaN value.                                        | NaN             | No        |
| path{i}_positive_inf                  | The string representation of a positive infinity value.                          | Inf             | No        |
| path{i}_negative_inf                  | The string representation of a negative infinity value.                          | -Inf            | No        |
| path{i}_sampling_ratio                | Fraction of input JSON objects used for schema inferring.                        | 1               | No        |
| path{i}_column_name_of_corrupt_record | Allows renaming the new field having malformed string created by PERMISSIVE mode |                 | No        |

For example:

```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 'csv',
  
  -- s3 credentials (optional) 
  aws_access_key 'my_key',
  aws_secret_key 'my_secret',
  aws_region     'eu-west-1',
  
  paths '2',
 
  path0_url 's3://bucket/path/*.csv',
  path0_header 'true',
  path0_delimiter ',',
  path0_null_value 'null',
   
  path1_url 's3://bucket/path/data2.csv',
  path1_header 'false',
);
```

## DAS JSON "json" Options

| Config Key                            | Description                                                                                                   | Default Value      | Mandatory |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------|--------------------|-----------|
| path{i}_multiline                     | Whether the JSON file is formatted as normal JSON (a single record may span multiple lines) or as JSON-lines. | true (normal JSON) | No        |
| path{i}_mode                          | The mode for parsing JSON files: PERMISSIVE, DROPMALFORMED, or FAILFAST .                                     | PERMISSIVE         | No        |
| path{i}_date_format                   | Custom date format for parsing date fields, e.g. yyyy-MM-d.                                                   |                    | No        |
| path{i}_timestamp_format              | Custom timestamp format, e.g. yyyy-MM-dd'T'HH:mm:ss.                                                          |                    | No        |
| path{i}_allow_comments                | Whether to allow comments in the JSON file. (default: false)                                                  | false              | No        |
| path{i}_drop_field_if_all_null        | Whether to drop fields that are always null (default: false).                                                 | false              | No        |
| path{i}_allow_unquoted_field_names    | Allows unquoted JSON field names. (default: false)                                                            | false              | No        |
| path{i}_sampling_ratio                | Fraction of input JSON objects used for schema inferring.                                                     | 1                  | No        |
| path{i}_column_name_of_corrupt_record | Allows renaming the new field having malformed string created by PERMISSIVE mode.                             |                    | No        |

For example:

```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 'json',
  
  -- github credentials (optional)
  github_api_token 'token'
  
  paths '2',
  
  path0_url 'github://owner/repo/branch/data1.json',
  path0_allow_comments 'true',
  path0_date_format 'yyyy-MM-d',
   
  path1_url 'github://owner/repo/branch/files/*.json'
);
```

## DAS XML "xml" Options

| Config Key                            | Description                                                                      | Default Value | Mandatory |
|---------------------------------------|----------------------------------------------------------------------------------|---------------|-----------|
| path{i}_row_tag                       | The tag for each row in the XML document.                                        |               | yes       |
| path{i}_root_tag                      | The tag for the root element in the XML document .                               |               | No        |
| path{i}_attribute_prefix              | Tag used to represent the element's text value when it has attributes.           |               | No        |
| path{i}_values_tag                    | Tag used to represent the element's text value when it has attributes.           |               | No        |
| path{i}_sampling_ratio                | Ratio of rows to use for schema inference .                                      | 1             | No        |
| path{i}_treat_empty_values_as_nulls   | Whether to treat empty string values as null.                                    |               | No        |
| path{i}_charset                       | Character encoding of the XML file .                                             | UTF-8         | No        |
| path{i}_mode                          | Error handling mode: PERMISSIVE, DROPMALFORMED, or FAILFAST.                     | PERMISSIVE    | No        |
| path{i}_date_format                   | Custom date format for parsing date fields, e.g. yyyy-MM-d.                      |               | No        |
| path{i}_timestampFormat               | Custom timestamp format, e.g. yyyy-MM-dd'T'HH:mm:ss .                            |               | No        |
| path{i}_column_name_of_corrupt_record | Allows renaming the new field having malformed string created by PERMISSIVE mode |               | No        |

For example:

```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 'xml',
  
  paths '2',
  
  path0_url 's3://bucket/path/discogs.xml',
  path0_row_tag 'artist',
   
  path1_url 's3://bucket/path/data2.xml',
  path1_row_tag 'item'
);
```

## DAS Parquet "parquet" Options

| Config Key                    | Description                                                                  | Default Value | Mandatory |
|-------------------------------|------------------------------------------------------------------------------|---------------|-----------|
| path{i}_merge_schema          | Whether to merge schemas from different files when reading from a directory. |               | No        |
| path{i}_recursive_file_lookup | Whether to recursively search subdirectories for Parquet files.              | false         | No        |
| path{i}_path_glob_filter      | Glob pattern to filter which files to read.                                  |               | No        |

For example:

```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 'parquet',
  
  paths '2',
  
  path0_url 's3://bucket/path/parquet1',
  path_glob_filter '*.parquet',
   
  path1_url 's3://bucket/path/parquet2',
  path0_merge_schema 'true'
);
```

## How to use

### Prerequisites

You need to have [sbt](https://www.scala-sbt.org/) installed to build the project.

You can install sbt using [sdkman](https://sdkman.io/):

```bash
$ sdk install sbt
```

### Running the server

You can run the server with the following command:

```bash
$ sbt run
```

### Docker

To run the server in a docker container you need to follow these steps:

First, you need to build the project:

```bash
$ sbt "docker:publishLocal"
```

Then you can run the image with the following command:

```bash
$ docker run -p 50051:50051 <image_id>
```

... where `<image_id>` is the id of the image created in the previous step.
This will start the server, typically on port 50051.

You can find the image id by looking at the sbt output or by running:

```bash
$ docker images
```