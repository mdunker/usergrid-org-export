SUMMARY:
Unsupported read-only tool for exporting data from Usergrid orgs. Uses the organization's credentials to download the data.

Data is retrieved into .jsonl files, where each line is a JSON object.

OPTIONS:
Options can be controlled via command line, but probably easier to control via a properties file.

protocol: http | https (default = https)
hostname: domain name plus port if necessary, can supply just domain name if on port 80/443 (default is localhost:8080)
org: Usergrid organization name (required)
app: Usergrid application name within the org (if not supplied, will export entire organization)
clientid: Usergrid organization client ID
clientsecret: Usergrid organization client secret
limit: number of entries to retrieve at a time (recommendation = default = 100)
perfile: number of entries per .jsonl file (default = 10000)
minfiledigits: number of digits per file, example first file is {collectionName}/0000.jsonl (default = 4)
destpath: path to create directory to store results (default is current directory)
ignore: apps/collections to ignore, app1/* (ignore app1), app1/coll1 (ignore coll1 in app1), */coll1 (ignore coll1 in any app) (default = ignore nothing)

USAGE:

Using properties file:
python orgexport.py --props org.props

Help:
python orgexport.py --help


OUTPUT FILES:

root directory = {orgName}_{date}_{time}

In root directory:
_org.json - JSON output of GET /management/orgs/{orgname}
{app} - directory for each application

In each app directory:
_app.json - JSON output of GET /management/orgs/{orgname}/apps/{appname}
_appstatus.json - current status of retrieval of Usergrid data, can use to determine apps that have been completed if export fails to complete, also contains count and range of modified and created dates for entities in the collection
{collection} - directory for each collection in the application
_connections - directory containing connections between entities (if connections exist)

In _connections directory:
{filenum}.jsonl - files containing one JSON connection per line, fields are sourceUuid, sourceType, targetUuid, targetType, connectionType
example: 
{"targetUuid":"91f08ab9-02fa-11e8-a836-122e0737977d","targetType":"skus","sourceUuid":"91af147f-02fa-11e8-a68e-0eec2415f3df","sourceType":"cart","connectionType":"contains"}

In each collection directory:
_settings.json - JSON output of GET /{org}/{app}/{collection}/_settings
_assets - contains one binary file per entity with an asset, filenames = {uuid}.{extension}, where extension is a guess based on the asset's file-metadata content-type, directory only exists if one or more entities in the collection have an associated asset
{filenum}.jsonl - files containing one JSON entity per line (GET /{org}/{app}/{collection}?limit={limit}&cursor={...})
