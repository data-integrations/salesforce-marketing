# Salesforce Marketing Cloud Source

Description
-----------

Reads from one or multiple objects within Salesforce Marketing Cloud depending on the mode values set for this plugin. 
In case of `Multi Object` mode, the source will output a record for each row from the object it reads, with each record
containing an additional field that holds the name of the object from which the record came from. In case of 
`Single Object` mode, this additional field will not be there in the output. In addition, for each object that will be 
read, this plugin will set pipeline arguments where the key is `multisink.[objectname]` and the value is the schema of 
the object.

Properties
----------

**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Data Retrieval Mode**: Mode of data retrieval. The mode can be one of two values: 

`Multi Object` - will allow user to fetch data for multiple data extensions,  

`Single Object` - will allow user to fetch data for single data extension.

**Object**: Specify the object for which data to be fetched. This can be one of following values: 

`Data Extension` - will allow user to fetch data for a single Data Extension object,

`Email` - will allow user to fetch data for Email object,

`Mailing List` - will allow user to fetch data for Mailing List object,

`Bounce Event` - will allow user to fetch Tracking Bounce Events,

`Open Event` - will allow user to fetch Tracking Open Events,

`Unsub Event` - will allow user to fetch Tracking UnSubscribe Events,

`Sent Event` - will allow user to fetch Tracking Sent Events,

`UnSent Event` - will allow user to fetch Tracking UnSent Events.

Note, this value will be ignored if the Mode is set to `Multi Object`.  

**Data Extension External Key**: Specify the data extension key from which data to be fetched. Note, this value will 
be ignored in following two cases: 

* If the Mode is set to `Multi Object`
* If the selected object name is other than `Data Extension`. 

**Object List**: Specify the comma-separated list of objects for which data to be fetched; for example: 
'Object1,Object2'. This can be one or more values from following possible values: 

`Data Extension` - will allow user to fetch data for a single Data Extension object,

`Email` - will allow user to fetch data for Email object,

`Mailing List` - will allow user to fetch data for Mailing List object

`Bounce Event` - will allow user to fetch Tracking Bounce Events,

`Open Event` - will allow user to fetch Tracking Open Events,

`Unsub Event` - will allow user to fetch Tracking UnSubscribe Events,

`Sent Event` - will allow user to fetch Tracking Sent Events,

`NotSent Event` - will allow user to fetch Tracking Not Sent Events.

Note, this value will be ignored if the Mode is set to `Single Object`.

**Data Extension External Keys**: Specify the data extension keys from which data to be fetched; for example: 
'Key1,Key2'. Note, this value will be ignored in following two cases: 

* If the Mode is set to `Single Object`,

* If the selected object list does not contain `Data Extension` as one of the objects.

**Table Name Field**: The name of the field that holds the object name to which the data belongs to. Must not be the 
name of any column for any of the objects that will be read. Defaults to `tablename`. In case of `Data Extension` 
object, this field will have value in `dataextension_[Data Extension Key]` format. Note, the Table name field value 
will be ignored if the Mode is set to `Single Object`.

**Filter**: The filter selection criteria. eg: eventDate > '2010-01-01' and subscriberKey = 1001

**Client ID**: OAuth2 client ID associated with an installed package in the Salesforce Marketing Cloud.

**Client Secret**: TOAuth2 client secret associated with an installed package in the Salesforce Marketing Cloud.

**Authentication Base URI**: Authentication Base URL associated for the Server-to-Server API integration. 
For example, `https://instance.auth.marketingcloudapis.com/`

**SOAP API Endpoint**: The SOAP Endpoint URL associated for the Server-to-Server API integration. For example, 
`https://instance.soap.marketingcloudapis.com/Service.asmx`

Data Type Mappings from Salesforce Marketing Cloud to CDAP
----------
The following table lists out different SFMC data types, as well as the
corresponding CDAP types. 

| SFMC type      | CDAP type     |
|----------------|---------------|
| string         | string        |
| phone          | string        |
| email_address  | string        |
| local          | string        |
| integer/number | int           |
| double/decimal | double        |
| boolean        | boolean       |
| timestamp      | timestamp     |