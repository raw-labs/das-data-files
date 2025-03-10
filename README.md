# DAS Data Files

[![License](https://img.shields.io/:license-BSL%201.1-blue.svg)](/licenses/BSL.txt)

[Data Access Service](https://github.com/raw-labs/protocol-das)
for reading CSV, JSON, XML, and Parquet files from HTTP/HTTPS URLs, or S3 (via s3a).
This plugin supports creating one or more “tables,” each representing a data file with
automatic schema inference (or metadata for Parquet).

## Overview

This DAS plugin defines multiple tables based on files from HTTP/HTTPS URLs or S3.:
The plugin supports the following file types:
* CSV
* JSON
* XML
* Parquet

A typical usage might look like:

```sql
SELECT id, name
FROM my_csv_data
WHERE id < 10
ORDER BY name ASC
```

## Available DAS Types

### DAS S3 CSV "s3-csv"


| Config Key                             | Description                                                                                        | Example                                             |
|----------------------------------------|----------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| aws_access_key                         | Access key for S3. (if not defined anonymous access is used)                                       |                                                     |
| aws_secret_key                         | Secret key for S3.                                                                                 |                                                     |
| nr_tables                              | The number of tables to define.                                                                    | nr_tables '2'                                       |
| table{i}_url                           | The path or URL to the file.  http:// or https://                                                  | table0_url 'https://host/data.csv'                  |
| table{i}_name                          | (Optional) The table name as seen in queries. Defaults to a name derived from the file’s URL.      | table0_name 'my_Table'                              |
| table{i}_*                             | Specific option for table`i` e.g. for csv `table0_header`                                          | table0_header 'true'                                |
| table{i}_header                        | Whether the first line is a header row (default: true).                                            | table0_header 'true'                                |
| table{i}_delimiter                     | Field delimiter character (default: ,)                                                             | table0_delimiter ','                                |
| table{i}_quote                         | Quote character for enclosed fields (default: ").                                                  | table0_quote '"'                                    |
| table{i}_escape                        | Escape character for quotes inside quoted fields (default: \\).                                    | table0_escape '\\'                                  |
| table{i}_multiline                     | Whether a single record can span multiple lines (default: false).                                  | table0_delimiter ','                                |
| table{i}_mode                          | The mode for parsing CSV files, one of PERMISSIVE, DROPMALFORMED, FAILFAST. (default: PERMISSIVE). | table0_mode 'PERMISSIVE'                            |
| table{i}_date_format                   | Custom date format for parsing date fields, e.g. yyyy-MM-d (optional).                             | table0_date_format 'yyyy-MM-d'                      |
| table{i}_timestamp_format              | Custom timestamp format, e.g. yyyy-MM-dd'T'HH:mm:ss (optional).                                    | table0_date_format 'yyyy-MM-dd''T''HH:mm:ss'        |

For example:
```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 's3-csv',
  
  -- s3 credentials (optional) 
  aws_access_key 'my_key',
  aws_secret_key 'my_secret',
  
  nr_tables '2',
  
  table0_url 's3://bucket/path/data1.csv',
  table0_header 'false',
   
  table1_url 's3://bucket/path/data2.csv',
);
```

### DAS S3 JSON "s3-json"

| Config Key                             | Description                                                                                         | Example                                             |
|----------------------------------------|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| aws_access_key                         | Access key for S3. (if not defined anonymous access is used)                                        |                                                     |
| aws_secret_key                         | Secret key for S3.                                                                                  |                                                     |
| nr_tables                              | The number of tables to define.                                                                     | nr_tables '2'                                       |
| table{i}_url                           | The path or URL to the file.  http:// or https://                                                   | table0_url 'https://host/data.csv'                  |
| table{i}_name                          | (Optional) The table name as seen in queries. Defaults to a name derived from the file’s URL.       | table0_name 'my_Table'                              |
| table{i}_*                             | Specific option for table`i` e.g. for csv `table0_header`                                           | table0_header 'true'                                |
| table{i}_multiline                     | Whether a single record can span multiple lines (default: true).                                    |                                                     |
| table{i}_mode                          | The mode for parsing JSON files, one of PERMISSIVE, DROPMALFORMED, FAILFAST. (default: PERMISSIVE). |                                                     |
| table{i}_date_format                   | Custom date format for parsing date fields, e.g. yyyy-MM-d (optional).                              |                                                     |
| table{i}_timestamp_format              | Custom timestamp format, e.g. yyyy-MM-dd'T'HH:mm:ss (optional)..                                    |                                                     |
| table{i}_allow_comments                | Whether to allow comments in the JSON file. (default: false)                                        |                                                     |
| table{i}_drop_field_if_all_null        | Whether to drop fields that are always null (optional).                                             |                                                     |
| table{i}_column_name_of_corrupt_record | Name for field holding corrupt records (optional).                                                  |                                                     |


For example:
```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 's3-json',
  
  -- s3 credentials (optional) 
  aws_access_key 'my_key',
  aws_secret_key 'my_secret',
  
  nr_tables '2',
  
  table0_url 's3://bucket/path/data1.json',
  table0_allow_comments 'true',
   
  table1_url 's3://bucket/path/data2.json',
);
```

### DAS S3 xml "s3-xml"


| Config Key                             | Description                                                                                                                | Example                                             |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| aws_access_key                         | Access key for S3. (if not defined anonymous access is used)                                                               |                                                     |
| aws_secret_key                         | Secret key for S3.                                                                                                         |                                                     |
| nr_tables                              | The number of tables to define.                                                                                            | nr_tables '2'                                       |
| table{i}_url                           | The path or URL to the file.  http:// or https://                                                                          | table0_url 'https://host/data.csv'                  |
| table{i}_name                          | (Optional) The table name as seen in queries. Defaults to a name derived from the file’s URL.                              | table0_name 'my_Table'                              |
| table{i}_*                             | Specific option for table`i` e.g. for csv `table0_header`                                                                  | table0_header 'true'                                |
| table{i}_row_tag                       | The tag for each row in the XML document (default: row).                                                                   |                                                     |
| table{i}_root_tag                      | The tag for the root element in the XML document (optional).                                                               |                                                     |
| table{i}_attribute_prefix              | Tag used to represent the element's text value when it has attributes (optional).                                          |                                                     | 
| table{i}_values_tag                    | Tag used to represent the element's text value when it has attributes (optional).                                          |                                                     |
| table{i}_sampling_ratio                | Ratio of rows to use for schema inference (between 0 and 1) (optional).                                                    |                                                     |
| table{i}_treat_empty_values_as_nulls   | Whether to treat empty string values as null (optional).                                                                   |                                                     |
| table{i}_charset                       | Character encoding of the XML file (default: UTF-8).                                                                       |                                                     |
| table{i}_mode                          | Error handling mode: PERMISSIVE, DROPMALFORMED, or FAILFAST.of PERMISSIVE, DROPMALFORMED, FAILFAST. (default: PERMISSIVE). |                                                     |
| table{i}_dateFormat                    | Custom date format for parsing date fields, e.g. yyyy-MM-d (optional).                                                     |                                                     |
| table{i}_timestampFormat               | Custom timestamp format, e.g. yyyy-MM-dd'T'HH:mm:ss (optional).                                                            |                                                     |

For example:
```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 's3-xml',
  
  -- s3 credentials (optional) 
  aws_access_key 'my_key',
  aws_secret_key 'my_secret',
  
  nr_tables '2',
  
  table0_url 's3://bucket/path/discogs.xml',
  table0_row_tag 'artist',
   
  table1_url 's3://bucket/path/data2.xml',
  table1_row_tag 'item',
);
```

### DAS S3 Parquet "s3-parquet"


| Config Key                     | Description                                                                                                                | Example                                             |
|--------------------------------|----------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| aws_access_key                 | Access key for S3. (if not defined anonymous access is used)                                                               |                                                     |
| aws_secret_key                 | Secret key for S3.                                                                                                         |                                                     |
| nr_tables                      | The number of tables to define.                                                                                            | nr_tables '2'                                       |
| table{i}_url                   | The path or URL to the file.  http:// or https://                                                                          | table0_url 'https://host/data.csv'                  |
| table{i}_name                  | (Optional) The table name as seen in queries. Defaults to a name derived from the file’s URL.                              | table0_name 'my_Table'                              |
| table{i}_*                     | Specific option for table`i` e.g. for csv `table0_header`                                                                  | table0_header 'true'                                |
| table{i}_merge_schema          | Whether to merge schemas from different files when reading from a directory (optional).                                    |
| table{i}_recursive_file_lookup | Whether to recursively search subdirectories for Parquet files (default false).                                            |
| table{i}_path_glob_filter      | Glob pattern to filter which files to read.                                                                                |


For example:
```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 's3-parquet',
  
  -- s3 credentials (optional) 
  aws_access_key 'my_key',
  aws_secret_key 'my_secret',
  
  nr_tables '2',
  
  table0_url 's3://bucket/path/parquet1',
  path_glob_filter '*.parquet',
   
  table1_url 's3://bucket/path/parquet2',
  table0_merge_schema 'true'
);
```

### DAS HTTP CSV "http-csv"

| Config Key                             | Description                                                                                        | Example                                             |
|----------------------------------------|----------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| http_follow_redirects                  | Whether to follow HTTP redirects (default: false).                                                 | http_follow_redirects 'true'                        |
| http_connect_timeout                   | HTTP connect timeout in milliseconds (default: 5000).                                              | http_connect_timeout_millis '10000'                 |
| http_ssl_trust_all                     | Whether to trust all SSL certificates (default: false).                                            | http_ssl_trust_all 'true'                           |
| nr_tables                              | The number of tables to define.                                                                    | nr_tables '2'                                       |
| table{i}_url                           | The path or URL to the file.  http:// or https://                                                  | table0_url 'https://host/data.csv'                  |
| table{i}_name                          | (Optional) The table name as seen in queries. Defaults to a name derived from the file’s URL.      | table0_name 'my_Table'                              |
| table{i}_*                             | Specific option for table`i` e.g. for csv `table0_header`                                          | table0_header 'true'                                |
| http table                             |                                                                                                    |                                                     |
| table{i}_http_method                   | http method (default: GET) (ignored if url is not http)                                            | table0_http_method 'POST'                           |
| table{i}_http_body                     | custom http body (ignored if url is not http)                                                      | table0_http_body '{"foo": "bar"}'                   |
| table{i}_http_header_*                 | custom http header (ignored if url is not http)                                                    | "table0_http_header_Authorization" 'Bearer <token>' |
| table{i}_http_read_timeout             | HTTP read timeout in milliseconds (default: 30000)                                                 | table0_http_read_timeout '10000'                    |
| table{i}_header                        | Whether the first line is a header row (default: true).                                            | table0_header 'true'                                |
| table{i}_delimiter                     | Field delimiter character (default: ,)                                                             | table0_delimiter ','                                |
| table{i}_quote                         | Quote character for enclosed fields (default: ").                                                  | table0_quote '"'                                    |
| table{i}_escape                        | Escape character for quotes inside quoted fields (default: \\).                                    | table0_escape '\\'                                  |
| table{i}_multiline                     | Whether a single record can span multiple lines (default: false).                                  | table0_delimiter ','                                |
| table{i}_mode                          | The mode for parsing CSV files, one of PERMISSIVE, DROPMALFORMED, FAILFAST. (default: PERMISSIVE). | table0_mode 'PERMISSIVE'                            |
| table{i}_date_format                   | Custom date format for parsing date fields, e.g. yyyy-MM-d (optional).                             | table0_date_format 'yyyy-MM-d'                      |
| table{i}_timestamp_format              | Custom timestamp format, e.g. yyyy-MM-dd'T'HH:mm:ss (optional).                                    | table0_date_format 'yyyy-MM-dd''T''HH:mm:ss'        |

For example:
```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 'http-csv',
  
  nr_tables '2',
  
  table0_url 'https://www.host/data1.csv',
  "table0_http_header_Authorization" 'Bearer <token>',
  table0_header 'false',
   
  table1_url 'https://host2/other/data2.csv',
  table1_http_read_timeout '20000'
);
```

### DAS http JSON "http-json"

| Config Key                             | Description                                                                                        | Example                                             |
|----------------------------------------|----------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| http_follow_redirects                  | Whether to follow HTTP redirects (default: false).                                                 | http_follow_redirects 'true'                        |
| http_connect_timeout                   | HTTP connect timeout in milliseconds (default: 5000).                                              | http_connect_timeout_millis '10000'                 |
| http_ssl_trust_all                     | Whether to trust all SSL certificates (default: false).                                            | http_ssl_trust_all 'true'                           |
| nr_tables                              | The number of tables to define.                                                                    | nr_tables '2'                                       |
| table{i}_url                           | The path or URL to the file.  http:// or https://                                                  | table0_url 'https://host/data.csv'                  |
| table{i}_name                          | (Optional) The table name as seen in queries. Defaults to a name derived from the file’s URL.      | table0_name 'my_Table'                              |
| table{i}_*                             | Specific option for table`i` e.g. for csv `table0_header`                                          | table0_header 'true'                                |
| http table                             |                                                                                                    |                                                     |
| table{i}_http_method                   | http method (default: GET) (ignored if url is not http)                                            | table0_http_method 'POST'                           |
| table{i}_http_body                     | custom http body (ignored if url is not http)                                                      | table0_http_body '{"foo": "bar"}'                   |
| table{i}_http_header_*                 | custom http header (ignored if url is not http)                                                    | "table0_http_header_Authorization" 'Bearer <token>' |
| table{i}_http_read_timeout             | HTTP read timeout in milliseconds (default: 30000)                                                 | table0_http_read_timeout '10000'                    |
| table{i}_multiline                     | Whether a single record can span multiple lines (default: true).                                   |                                                     |
| table{i}_mode                          | The mode for parsing CSV files, one of PERMISSIVE, DROPMALFORMED, FAILFAST. (default: PERMISSIVE). |                                                     |
| table{i}_date_format                   | Custom date format for parsing date fields, e.g. yyyy-MM-d (optional).                             |                                                     |
| table{i}_timestamp_format              | Custom timestamp format, e.g. yyyy-MM-dd'T'HH:mm:ss (optional)..                                   |                                                     |
| table{i}_allow_comments                | Whether to allow comments in the JSON file. (default: false)                                       |                                                     |
| table{i}_drop_field_if_all_null        | Whether to drop fields that are always null (optional).                                            |                                                     |
| table{i}_column_name_of_corrupt_record | Name for field holding corrupt records (optional).                                                 |                                                     |

For example:
```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 'http-json',
  
  nr_tables '2',
  
  table0_url 'https://www.host/data1.json',
  "table0_http_header_Authorization" 'Bearer <token>',
  table0_allow_comments 'true',
   
  table1_url 'https://host2/path/data2.json',
  table1_http_read_timeout '20000'

);
```

### DAS http xml "http-xml"

| Config Key                             | Description                                                                                                                | Example                                             |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| http_follow_redirects                  | Whether to follow HTTP redirects (default: false).                                                                         | http_follow_redirects 'true'                        |
| http_connect_timeout                   | HTTP connect timeout in milliseconds (default: 5000).                                                                      | http_connect_timeout_millis '10000'                 |
| http_ssl_trust_all                     | Whether to trust all SSL certificates (default: false).                                                                    | http_ssl_trust_all 'true'                           |
| nr_tables                              | The number of tables to define.                                                                                            | nr_tables '2'                                       |
| table{i}_url                           | The path or URL to the file.  http:// or https://                                                                          | table0_url 'https://host/data.csv'                  |
| table{i}_name                          | (Optional) The table name as seen in queries. Defaults to a name derived from the file’s URL.                              | table0_name 'my_Table'                              |
| table{i}_*                             | Specific option for table`i` e.g. for csv `table0_header`                                                                  | table0_header 'true'                                |
| http table                             |                                                                                                                            |                                                     |
| table{i}_http_method                   | http method (default: GET) (ignored if url is not http)                                                                    | table0_http_method 'POST'                           |
| table{i}_http_body                     | custom http body (ignored if url is not http)                                                                              | table0_http_body '{"foo": "bar"}'                   |
| table{i}_http_header_*                 | custom http header (ignored if url is not http)                                                                            | "table0_http_header_Authorization" 'Bearer <token>' |
| table{i}_http_read_timeout             | HTTP read timeout in milliseconds (default: 30000)                                                                         | table0_http_read_timeout '10000'                    |
| table{i}_row_tag                       | The tag for each row in the XML document (default: row).                                                                   |                                                     |
| table{i}_root_tag                      | The tag for the root element in the XML document (optional).                                                               |                                                     |
| table{i}_attribute_prefix              | Tag used to represent the element's text value when it has attributes (optional).                                          |                                                     | 
| table{i}_values_tag                    | Tag used to represent the element's text value when it has attributes (optional).                                          |                                                     |
| table{i}_sampling_ratio                | Ratio of rows to use for schema inference (between 0 and 1) (optional).                                                    |                                                     |
| table{i}_treat_empty_values_as_nulls   | Whether to treat empty string values as null (optional).                                                                   |                                                     |
| table{i}_charset                       | Character encoding of the XML file (default: UTF-8).                                                                       |                                                     |
| table{i}_mode                          | Error handling mode: PERMISSIVE, DROPMALFORMED, or FAILFAST.of PERMISSIVE, DROPMALFORMED, FAILFAST. (default: PERMISSIVE). |                                                     |
| table{i}_dateFormat                    | Custom date format for parsing date fields, e.g. yyyy-MM-d (optional).                                                     |                                                     |
| table{i}_timestampFormat               | Custom timestamp format, e.g. yyyy-MM-dd'T'HH:mm:ss (optional).                                                            |                                                     |


For example:
```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 'http-xml',
  
  nr_tables '2',
  
  table0_url 'https://www.host/discogs.xml',
  "table0_http_header_Authorization" 'Bearer <token>',
  table0_row_tag 'artist',
   
  table1_url 'https://host2/path/data2.xml',
  table1_row_tag 'item',
);
```

### DAS http Parquet "http-parquet"


| Config Key                 | Description                                                                                                                | Example                                             |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| http_follow_redirects      | Whether to follow HTTP redirects (default: false).                                                                         | http_follow_redirects 'true'                        |
| http_connect_timeout       | HTTP connect timeout in milliseconds (default: 5000).                                                                      | http_connect_timeout_millis '10000'                 |
| http_ssl_trust_all         | Whether to trust all SSL certificates (default: false).                                                                    | http_ssl_trust_all 'true'                           |
| nr_tables                  | The number of tables to define.                                                                                            | nr_tables '2'                                       |
| table{i}_url               | The path or URL to the file.  http:// or https://                                                                          | table0_url 'https://host/data.csv'                  |
| table{i}_name              | (Optional) The table name as seen in queries. Defaults to a name derived from the file’s URL.                              | table0_name 'my_Table'                              |
| table{i}_*                 | Specific option for table`i` e.g. for csv `table0_header`                                                                  | table0_header 'true'                                |
| http table                 |                                                                                                                            |                                                     |
| table{i}_http_method       | http method (default: GET) (ignored if url is not http)                                                                    | table0_http_method 'POST'                           |
| table{i}_http_body         | custom http body (ignored if url is not http)                                                                              | table0_http_body '{"foo": "bar"}'                   |
| table{i}_http_header_*     | custom http header (ignored if url is not http)                                                                            | "table0_http_header_Authorization" 'Bearer <token>' |
| table{i}_http_read_timeout | HTTP read timeout in milliseconds (default: 30000)                                                                         | table0_http_read_timeout '10000'                    |

For example:
```sql
CREATE SERVER datafiles FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051',
  das_type 'http-parquet',
  
  -- HTTP connection settings (optional)
  http_follow_redirects 'true',
  http_connect_timeout_millis '10000',
  http_ssl_trust_all 'true',
  
  nr_tables '2',
  
  table0_url 'https://www.host/data1.parquet',
  "table0_http_header_Authorization" 'Bearer <token>',
   
  table1_url 'https://host2/path/parquet2',
);
```

## How to Build & Run

1: Build the Project

Use sbt:

```bash
sbt "project docker" "docker:publishLocal"
```

This creates a Docker image, typically named das-datafiles.

2: Run the Docker Image

```bash
docker run -p 50051:50051 <image_id>
```

Where `<image_id>` is from the docker images list (the ID of your freshly built image). This starts the DAS plugin
server on port 50051.

Query the DAS

In your environment that supports the Data Access Service (e.g., raw-labs CLI or platform), you can query the tables you
defined. Example:


