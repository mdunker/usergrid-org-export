from __future__ import print_function
import json
import logging
import sys
import os
import errno
import uuid
import httplib
import urllib
import base64
import argparse
import time
import mimetypes

LOGFILE="orgexport.log"
logFile = open(LOGFILE, 'w')

runUuid = uuid.uuid1()
runQueryParam = 'orgexport'

def logOnly(str):
	logFile.write(str + '\n')

def log(str):
	print(str)
	logFile.write(str + '\n')

#def printErr(*args, **kwargs):
	#print(*args, file=sys.stderr, **kwargs)

def getTimeString():
	return time.strftime('%Y%m%d_%H%M%S', time.gmtime())

def representsInt(s):
	try:
		int(s)
		return True
	except ValueError:
		return False

def makeDir(path):
	try:
		os.makedirs(path)
	except OSError as e:
		if e.errno != errno.EEXIST:
			raise

def exitWithError(s):
	log("ERROR: " + s)
	sys.exit(1)

def loadProperties(path,sep='=',comment='#'):
	props = {}
	with open(path, "rt") as f:
		for line in f:
			l = line.split(comment, 1)[0].strip()
			if l:
				keyAndValue = l.split(sep)
				key = keyAndValue[0].strip()
				value = keyAndValue[1].strip().strip('"')
				props[key] = value
	return props

def openFile(fileName):
	return open(fileName, 'w')

def openAppendFile(fileName):
	return open(fileName, 'a')

def getPrettyJsonStr(obj):
	return json.dumps(obj, indent=2)

def getJsonStr(obj):
	return json.dumps(obj, separators=(',',':')) 

def appendJson(f, obj):
	f.write(getPrettyJsonStr(obj) + '\n')

def appendJsonList(f, jsonStrings):
	f.writelines('{}\n'.format(x) for x in jsonStrings)
	
def writeSingleJsonFile(fileName, obj):
	f = openFile(fileName)
	appendJson(f, obj)
	f.close()

def writeMultipleJsonFile(collectionRoot, fNum, jsonStrings):
	f = getCollectionFile(collectionRoot, fNum)
	appendJsonList(f, jsonStrings)
	f.close()

def writeMultipleJsonConnectionFile(appRoot, fNum, jsonStrings):
	f = getConnectionFile(appRoot, fNum)
	appendJsonList(f, jsonStrings)
	f.close()

NOTSTARTED='NOTSTARTED'
INPROGRESS='INPROGRESS'
COMPLETE='COMPLETE'
SKIPPED='SKIPPED'
EMPTY='EMPTY'

appsStatus = {}

def initAppStatus(appName, collectionNames):
	appStatus = {}
	appStatus['status'] = NOTSTARTED
	collections = {}
	for collectionName in collectionNames:
		info = {}
		info['status'] = NOTSTARTED
		info['count'] = 0
		createdRange = {}
		createdRange['earliest'] = None
		createdRange['latest'] = None
		info['created'] = createdRange
		modifiedRange = {}
		modifiedRange['earliest'] = None
		modifiedRange['latest'] = None
		info['modified'] = modifiedRange
		collections[collectionName] = info
	appStatus['collections'] = collections
	appStatus['emptyCollections'] = []
	appsStatus[appName] = appStatus

def writeAppStatus(appName):
	fileName = '{}/{}/{}'.format(root, appName, '_appstatus.json')
	writeSingleJsonFile(fileName, appsStatus[appName])

def updateCollectionInfo(appName, collectionName, count, earliestCreated, latestCreated, earliestModified, latestModified):
	appsStatus[appName]['collections'][collectionName]['count'] = count
	appsStatus[appName]['collections'][collectionName]['created']['earliest'] = earliestCreated
	appsStatus[appName]['collections'][collectionName]['created']['latest'] = latestCreated
	appsStatus[appName]['collections'][collectionName]['modified']['earliest'] = earliestModified
	appsStatus[appName]['collections'][collectionName]['modified']['latest'] = latestModified

def updateCollectionStatus(appName, collectionName, status):
	if status == EMPTY:
		appsStatus[appName]['collections'].pop(collectionName, None)
		appsStatus[appName]['emptyCollections'].append(collectionName)
	else:
		appsStatus[appName]['collections'][collectionName]['status'] = status

def updateAppStatus(appName, status):
	appsStatus[appName]['status'] = status

def getCollectionFile(collectionRoot, fNum):
	return openFile('{0}/{1:0{width}}.jsonl'.format(collectionRoot, fNum, width=minFileDigits))

def getConnectionFile(appRoot, fNum):
	return openAppendFile('{0}/_connections/{1:0{width}}.jsonl'.format(appRoot, fNum, width=minFileDigits))

def writeCollectionStatus(prefix, periodCount):
	sys.stdout.write('{}{}\r'.format(prefix, '.' * periodCount))
	sys.stdout.flush()

def timestampMin(arg1, arg2):
	if arg1 is None:
		return arg2
	elif arg2 is None:
		return arg1
	elif arg1 < arg2:
		return arg1
	else:
		return arg2

def timestampMax(arg1, arg2):
	if arg1 is None:
		return arg2
	elif arg2 is None:
		return arg1
	elif arg1 > arg2:
		return arg1
	else:
		return arg2

parser = argparse.ArgumentParser(description='Get all data for a Usergrid org/app. You can use any of these arguments (except --props) in a provided properties file.')
parser.add_argument('--props', help='path to properties file')
parser.add_argument('--protocol', help='http or https')
parser.add_argument('--hostname', help='hostname for Usergrid requests')
parser.add_argument('--org', help='organization name')
parser.add_argument('--app', help='application name')
parser.add_argument('--clientid', help='client id')
parser.add_argument('--clientsecret', help='client secret')
parser.add_argument('--limit', type=int, help='number of entities to retrieve at a time')
parser.add_argument('--perfile', type=int, help='num entities per file')
parser.add_argument('--minfiledigits', type=int, help='minimum digits in entities filename (4 -> start with 0001)')
parser.add_argument('--destpath', help='path to create directory for storing results')
parser.add_argument('--ignore', help='apps (myapp/*) or collections (myapp/collname or */collname) to ignore, comma-separated')
args = parser.parse_args()

maxPerFile = 1000000
maxLimit = 1000
maxMinFileDigits = 12
fileExtension = 'json'

# default values
isHttps = True
defaultHostName = 'localhost:8080'
hostName = defaultHostName
org = None
specifiedApp = None
clientId = None
clientSecret = None
limit = 100
perFile = 10000
minFileDigits = 4
props = None
destPath = '.'
specifiedIgnoreList = ''
saveEntities = True
knownExtensions = { "text/plain": ".txt" }

if args.props:
	props = loadProperties(args.props)
	if 'protocol' in props:
		protocol = props['protocol']
		if protocol != 'http' and protocol != 'https':
			exitWithError('protocol in props file must be http or https')
		isHttps = (protocol == 'https')
	if 'hostname' in props:
		hostName = props['hostname']
	if 'org' in props:
		org = props['org']
	if 'app' in props:
		specifiedApp = props['app']
	if 'clientid' in props:
		clientId = props['clientid']
	if 'clientsecret' in props:
		clientSecret = props['clientsecret']
	if 'limit' in props:
		if not representsInt(props['limit']):
			exitWithError('limit in props file not an integer')
		limit = int(props['limit'])
		if limit < 1 or limit > maxLimit:
			exitWithError('limit in props file out of range, must be between 1 and {}'.format(maxLimit))
	if 'perfile' in props:
		if not representsInt(props['perfile']):
			exitWithError('perfile in props file not an integer')
		perFile = int(props['perfile'])
		if perFile < 1 or perFile > maxPerFile:
			exitWithError('perfile in props file out of range, must be between 1 and {}'.format(maxPerFile))
	if 'minentdigits' in props:
		if not representsInt(props['minentdigits']):
			exitWithError('minentdigits in props file not an integer')
		minFileDigits = int(props['minentdigits'])
		if minFileDigits < 1 or minFileDigits > maxMinFileDigits:
			exitWithError('minentdigits in props file out of range, must be between 1 and {}'.format(maxMinFileDigits))
	if 'destpath' in props:
		destPath = props['destpath']
	if 'ignore' in props:
		specifiedIgnoreList = props['ignore']

if args.protocol:
	if args.protocol != 'http' and args.protocol != 'https':
		exitWithError('protocol argument must be http or https')
	isHttps = (args.protocol == 'https')
hostName = args.hostname or hostName
org = args.org or org
specifiedApp = args.app or specifiedApp
clientId = args.clientid or clientId
clientSecret = args.clientsecret or clientSecret
limit = args.limit or limit
perFile = args.perfile or perFile
minFileDigits = args.minfiledigits or minFileDigits
destPath = args.destpath or destPath
specifiedIgnoreList = args.ignore or specifiedIgnoreList

ignoreList = [i.strip() for i in specifiedIgnoreList.split(',')]
log('ignoreList: {}'.format(ignoreList))

def shouldIgnoreApp(app):
	if '{}/*'.format(app) in ignoreList:
		return True
	return False

def shouldIgnoreColl(app, coll):
	if '{}/{}'.format(app, coll) in ignoreList or '*/{}'.format(coll) in ignoreList:
		return True
	return False

# open connection to org database
if isHttps:
	conn = httplib.HTTPSConnection(hostName)
else:
	conn = httplib.HTTPConnection(hostName)

base64AuthString = base64.b64encode('{}:{}'.format(clientId, clientSecret))
headers = {
	"Authorization": 'Basic {}'.format(base64AuthString),
	"Content-Type": "application/json"
}
headersNoPayload = {
	"Authorization": 'Basic {}'.format(base64AuthString),
}

def callNoPayload(verb, path):
	conn.request(verb, path, None, headersNoPayload)
	callResponse = conn.getresponse()
	responsePayload = callResponse.read()
	responseObj = None
	try:
		responseObj = json.loads(responsePayload)
	except:
		pass
	return callResponse.status, responseObj, responsePayload

def callGet(path):
	return callNoPayload("GET", path)

def getData(src, chunkSize=1024):
	d = src.read(chunkSize)
	while d:
		yield d
		d = src.read(chunkSize)

def makeAllDirectories(filename):
	if not os.path.exists(os.path.dirname(filename)):
	    try:
		os.makedirs(os.path.dirname(filename))
	    except OSError as exc: # Guard against race condition
		if exc.errno != errno.EEXIST:
		    raise

def callBinaryGet(path, accept, filename):
	makeAllDirectories(filename)

	fullPath = '{}?{}={}&assetAccept={}'.format(path, runQueryParam, runUuid, accept)

	conn.request("GET", fullPath, None, {'Authorization': 'Basic {}'.format(base64AuthString), 'Accept': accept})
	callResponse = conn.getresponse()
	if callResponse.status == 200:
		file = open(filename, 'wb')
		for chunk in getData(callResponse):
			bytes = bytearray(chunk)
			file.write(bytes)
		file.close()
	else:
		log('Failed to retrieve file {}'.format(filename))

def callWithPayload(verb, path, payloadObj):
	#log(json.dumps(payloadObj))
	conn.request(verb, path, json.dumps(payloadObj), headers)
	callResponse = conn.getresponse()
	responsePayload = callResponse.read()
	#log(responsePayload)
	responseObj = None
	try:
		responseObj = json.loads(responsePayload)
	except:
		pass
	return callResponse.status, responseObj, responsePayload

def callPut(path, payloadObj):
	return callWithPayload("PUT", path, payloadObj)

def callPost(path, payloadObj):
	return callWithPayload("POST", path, payloadObj)

def getNext(app, coll, cursor):
	if cursor is None:
		cursorStr = ""
	else:
		cursorStr = "&cursor={}".format(cursor)
	path = '/{}/{}/{}?{}={}&limit={}{}'.format(org, app, coll, runQueryParam, runUuid, limit, cursorStr)
	return callGet(path)

def getNextPath(path, cursor):
	if cursor is None:
		cursorStr = ""
	else:
		cursorStr = "&cursor={}".format(cursor)
	fullPath = '{}?{}={}&limit={}{}'.format(path, runQueryParam, runUuid, limit, cursorStr)
	return callGet(fullPath)

def getManagementOrg():
	status, responseObj, responsePayload = callGet('/management/orgs/{}?{}={}'.format(org, runQueryParam, runUuid))
	responseObj.pop('duration',0)
	responseObj.pop('timestamp',0)
	return status, responseObj, responsePayload

def getManagementApp(app):
	status, responseObj, responsePayload = callGet('/management/orgs/{}/apps/{}?{}={}'.format(org, app, runQueryParam, runUuid))
	responseObj.pop('action',0)
	responseObj.pop('duration',0)
	responseObj.pop('params',0)
	responseObj.pop('timestamp',0)
	return status, responseObj, responsePayload

def getCollectionSettings(app, collection):
	status, responseObj, responsePayload = callGet('/{}/{}/{}/_settings?{}={}'.format(org, app, collection, runQueryParam, runUuid))
	responseObj.pop('action',0)
	responseObj.pop('duration',0)
	responseObj.pop('entities',0)
	responseObj.pop('params',0)
	responseObj.pop('timestamp',0)
	return status, responseObj, responsePayload

def getAsset(app, collection, entityUuid, contentType, collectionRoot):
	assetPath = '/{}/{}/{}/{}'.format(org, app, collection, entityUuid)
	extension = knownExtensions.get(contentType) or mimetypes.guess_extension(contentType, False) or '.asset'
	log('\n  Asset: {} ({}) - {}\n'.format(contentType, extension, assetPath))
	assetFileName = '{}/_assets/{}{}'.format(collectionRoot, entityUuid, extension)
	callBinaryGet(assetPath, contentType, assetFileName)

# find apps
appList = []
status, orgPayloadObj, orgPayloadStr = getManagementOrg()
#log(payloadStr)

if specifiedApp == None:
	try:
		orgApps = orgPayloadObj['organization']['applications']
		for orgAppName, appUuid in orgApps.iteritems():
			log('App: {} ({})'.format(orgAppName, appUuid))
			appList.append(orgAppName.split('/')[1])

	except:
		exitWithError("failed to retrieve application list from org object: {0}".format(orgPayloadStr))
else:
	appList.append(specifiedApp)


root = destPath
if not destPath.endswith('/'):
	root += '/'

orgDirectoryName = '{}_{}'.format(org, getTimeString())
#log(orgDirectoryName)
root = '{}{}'.format(root, orgDirectoryName)
makeDir(root)

writeSingleJsonFile('{}/{}'.format(root, '_org.json'), orgPayloadObj)

log('AppList: {}'.format(', '.join(appList)))

for app in appList:
	if shouldIgnoreApp(app):
		log('IGNORING APP {}'.format(app))
		continue
	# get app information, including collections
	collections = []
	status, payloadObj, payloadStr = getManagementApp(app)
	try:
		#log(payloadStr)
		appRoot = '{}/{}'.format(root, app)
		makeDir(appRoot)
		writeSingleJsonFile('{}/{}'.format(appRoot, '_app.json'), payloadObj)

		collectionsPart = payloadObj['entities'][0]['metadata']['collections']
		for collectionName, collectionDetails in collectionsPart.iteritems():
			#log('Collection {} ({})'.format(collectionName, collectionDetails['count']))
			collections.append(collectionName)
	except:
		exitWithError("failed to retrieve collection list from app object: {0}".format(payloadStr))
	
	initAppStatus(app, collections)
	writeAppStatus(app)

	log('App: {}'.format(app))

	appConnections = 0
	fileConnections = 0
	connectionFileNum = 0
	connectionJsonStrings = []
	connectionInitComplete = False


	for collection in collections:
		if shouldIgnoreColl(app, collection):
			log('IGNORING COLLECTION {}/{}'.format(app, collection))
			updateCollectionStatus(app, collection, SKIPPED)
			continue
		
		try:
			# retrieve data
			collectionEntities = 0
			collectionFileEntities = 0
			collectionFileNum = 0
			cursor = None
			collectionFile = None
			earliestCreated = None
			latestCreated = None
			earliestModified = None
			latestModified = None
			jsonStrings = []
			collectionInitComplete = False

			collectionPrefix = ' Collection: {}'.format(collection)
			writeCollectionStatus(collectionPrefix, collectionFileNum)
			logOnly('Collection: {}'.format(collection))

			while True:
				status, payloadObj, payloadStr = getNext(app, collection, cursor)
				if 'entities' in payloadObj:
					for entity in payloadObj['entities']:
						if not collectionInitComplete:
							# make collection directory
							collectionRoot = '{}/{}'.format(appRoot, collection)
							makeDir(collectionRoot)

							# retrieve and write collection settings
							status, collSettingsPayloadObj, collSettingsPayloadStr = getCollectionSettings(app, collection)
							writeSingleJsonFile('{}/_settings.json'.format(collectionRoot), collSettingsPayloadObj)

							updateCollectionStatus(app, collection, INPROGRESS)
							collectionInitComplete = True
						jsonStrings.append(getJsonStr(entity))
						earliestCreated = timestampMin(earliestCreated, entity['created'])
						latestCreated = timestampMax(latestCreated, entity['created'])
						earliestModified = timestampMin(earliestModified, entity['modified'])
						latestModified = timestampMax(latestModified, entity['modified'])
						entityUuid = entity['uuid']
						collectionEntities += 1

						if 'file-metadata' in entity:
							# asset
							fileMetadata = entity['file-metadata']
							if 'content-type' in fileMetadata:
								contentType = fileMetadata['content-type']
								getAsset(app, collection, entityUuid, contentType, collectionRoot)

						# search for connections from the source
						if 'metadata' in entity and 'connections' in entity['metadata']:
							connectionTypes = entity['metadata']['connections']
							entityType = entity['type']
							for connectionType, connectionPath in connectionTypes.iteritems():
								connectionsPath = '/{}/{}{}'.format(org, app, connectionPath)
								connectionCursor = None
								while True:
									status, connectionsPayloadObj, connectionsPayloadStr = getNextPath(connectionsPath, connectionCursor)
									#print('\n')
									#print(json.dumps(connectionsPayloadObj))
									if 'entities' in connectionsPayloadObj:
										connectedEntities = connectionsPayloadObj['entities']
										for connectedEntity in connectedEntities:
											appConnections += 1
											if not connectionInitComplete:
												makeDir('{}/_connections'.format(appRoot))
												connectionInitComplete = True
											connectedEntityUuid = connectedEntity['uuid']
											connectedEntityType = connectedEntity['type']
											#print('{} ({})'.format(connectedEntityUuid, connectedEntityType))
											newConnection = {}
											newConnection['sourceUuid'] = entityUuid
											newConnection['sourceType'] = entityType
											newConnection['connectionType'] = connectionType
											newConnection['targetUuid'] = connectedEntityUuid
											newConnection['targetType'] = connectedEntityType
											connectionJsonStrings.append(getJsonStr(newConnection))
											fileConnections += 1
											if fileConnections >= perFile:
												writeMultipleJsonConnectionFile(appRoot, connectionFileNum, connectionJsonStrings)
												connectionJsonStrings = []
												connectionFileNum += 1
												fileConnections = 0
									if 'cursor' in connectionsPayloadObj:
										connectionCursor = connectionsPayloadObj['cursor']
									else:
										break

						collectionFileEntities += 1
						if collectionFileEntities >= perFile:
							# write file
							writeMultipleJsonFile(collectionRoot, collectionFileNum, jsonStrings)

							jsonStrings = []
							collectionFileNum += 1
							collectionFileEntities = 0

							updateCollectionInfo(app, collection, collectionEntities, earliestCreated, latestCreated, earliestModified, latestModified)
							writeAppStatus(app)
							writeCollectionStatus(collectionPrefix, collectionFileNum)
				if 'cursor' in payloadObj:
					cursor = payloadObj['cursor']
				else:
					break


			if collectionEntities > 0:
				if collectionFileEntities > 0:
					writeMultipleJsonFile(collectionRoot, collectionFileNum, jsonStrings)
					collectionFileNum += 1
					writeCollectionStatus(collectionPrefix, collectionFileNum)
				updateCollectionInfo(app, collection, collectionEntities, earliestCreated, latestCreated, earliestModified, latestModified)
				updateCollectionStatus(app, collection, COMPLETE)
			else:
				updateCollectionStatus(app, collection, EMPTY)
			writeAppStatus(app)
			sys.stdout.write('\n')
			sys.stdout.flush()


		except:
			writeAppStatus(app)
			
	if appConnections > 0:
		if fileConnections > 0:
			writeMultipleJsonConnectionFile(appRoot, connectionFileNum, connectionJsonStrings)
			connectionFileNum += 1

	updateAppStatus(app, COMPLETE)
	writeAppStatus(app)


log('SAVED TO DIRECTORY: {}'.format(root))
log('Run UUID: {}'.format(runUuid))

logFile.close()
sys.exit(0)

