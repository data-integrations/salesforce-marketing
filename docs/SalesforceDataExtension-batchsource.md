# Salesforce Marketing Cloud Data Extension Source

Description
-----------

Reads from one or multiple data extensions within Salesforce Marketing Cloud depending on the mode values set for 
this plugin. In case of `Multi Object` mode, the source will output a record for each row in the data extension it 
reads, with each record containing an additional field that holds the name of the data extension from which the record 
came from. In case of `Single Object` mode, this additional field will not be there in the output. In addition, for 
each data extension that will be read, this plugin will set pipeline arguments where the key is 
`multisink.[dataextensionname]` and the value is the schema of the data extension.
  

Properties
----------

**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Data Retrieval Mode**: Mode of data retrieval. The mode can be one of two values: 

`Multi Object` - will allow user to fetch data for multiple data extensions,  

`Single Object` - will allow user to fetch data for single data extension.

**Data Extension External Key**: Specify the data extension key from which data to be fetched. Note, this value will 
be ignored if the Mode is set to `Multi Object`.  

**Data Extension External Keys**: Specify the data extension keys from which data to be fetched; 
for example: 'Key1,Key2'.

Note, this value will be ignored if the Mode is set to `Single Object`.

**Table Name Field**: The name of the field that holds the data extension name. Must not be the name of any data 
extension column that will be read. Defaults to `tablename`. Note, the Table name field value will be ignored if the 
Mode is set to `Single Object`.

**Client ID**: OAuth2 client ID associated with an installed package in the Salesforce Marketing Cloud.

**Client Secret**: TOAuth2 client secret associated with an installed package in the Salesforce Marketing Cloud.

**Authentication Base URI**: Authentication Base URL associated for the Server-to-Server API integration. 
For example, `https://instance.auth.marketingcloudapis.com/`

**SOAP API Endpoint**: The SOAP Endpoint URL associated for the Server-to-Server API integration. For example, 
`https://instance.soap.marketingcloudapis.com/Service.asmx`

**REST API Base URI**: The REST API Base URL associated for the Server-to-Server API integration. For example, 
`https://instance.rest.marketingcloudapis.com/`
