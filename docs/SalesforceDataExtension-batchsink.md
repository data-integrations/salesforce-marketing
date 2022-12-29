# Salesforce Marketing Cloud Data Extension Sink


Description
-----------
This sink inserts records into a Salesforce Marketing Cloud Data Extension.
The sink requires Server-to-Server integration with the Salesforce Marketing Cloud API. See
https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/api-integration.htm
for more information about creating an API integration.

Records are written in batches to the Data Extension. By default, any errors inserting the
records will be logged and ignored, though the sink can be configured to fail the pipeline if
it encounters an error. However, insertions are not atomic, so a failure to write a record
midway through the pipeline will result in partial writes being made to the Data Extension.

Input fields must be named the same as they are named in the Data Extension, or they must
be mapped to the corresponding column in the Data Extension.
Input fields must be of a type compatible with the column type in the Data Extension.

  - If the column type is text, phone, email, or locale, the input field must be a string
  - If the column type is boolean, the input field must be a boolean or a string representing a boolean.
  - If the column type is date, the input field must be a date or a string representing a date in a supported format
  - If the column type is number, the input field must be an int or a string representing an int
  - If the column type is a decimal, the input field must be a decimal or a string representing a decimal

The sink can be configured to either insert, update, or upsert records. An upsert is implemented as
an attempted insert followed by an update for any record that failed due to a primary key constraint violation.

Configuration
-------------
**Use Connection:** Whether to use a connection. If a connection is used, you do not need to provide the credentials.

**Connection:** Name of the connection to use. Object Names information will be provided by the connection.
You also can use the macro function ${conn(connection-name)}.

**Client ID:** OAuth2 client ID associated with an installed package in the Salesforce Marketing Cloud.

**Client Secret:** OAuth2 client secret associated with an installed package in the Salesforce Marketing Cloud.

**Authentication Base URI:** Authentication Base URL associated for the Server-to-Server API integration.

**SOAP Base URI:** Authentication Base URL associated for the Server-to-Server API integration.

**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Data Extension External Key:** External key of the Data Extension to write to.

**Operation:** Whether to insert, update, or upsert records in the Data Extension.

**Truncate Text:** Whether to truncate text that is longer than the max length specified in the data extension column.

**Fail On Error:** Whether to fail the pipeline if an error is encountered while inserting records into
the Data Extension.

**Replace Underscores With Spaces:** Whether to replace underscores in the input field names with spaces
when writing to the data extension. For example, if an input field is named 'User_ID', it will be written
to the data extension column 'User ID'.

**Max Batch Size:** Maximum number of records to batch together in a write. Batching is used to improve
write performance. Records in a batch are not applied atomically. This means some records in a batch
may be written successfully while others in the batch may fail.

**Column Mapping:** Mapping from input field name to its corresponding column in the Data Extension.
For example, this can be used to indicate that the input field named 'email' should be written to
a column named 'customer email'.
