#!/usr/bin/env python3
#
# PoC code: no warranty, might explode, harmful to children, etc
#
# File structure of a DRDL:
# schema:
# - db: apt
#   tables:
#   - table: TABLE_NAME
#     collection: COLLECTION_NAME
#     pipeline:
#     - $stage1: [...]
#     - $stage2: [...]
#     columns: [{ MongoType, Name, SqlName, SqlType }]
#
# TODO: 
# - handle entities under recursive objects, eg: 
#   - quote_coverageList_insuredEntityList
#   - quote_coverageList_coverageList_insuredEntityList
# - handle case when multiple objects are recursive, eg: quote_coverageList_coverageList_insuredEntityList_insuredEntityList
# - remove these constants:
COLL_NAME = "quotes"
ID_NAME = "oid"
SRC_FILENAME = "src.drdl"
DST_FILENAME = "dst.drdl"
# END TODO


import yaml
import re

def removeDuplicates(x):
    return list(dict.fromkeys(x))

def getDocumentClass(tableName):
    return tableName.split("_")[-1].replace("List", "")

def getDocumentParentClass(tableName):
    elements = removeDuplicates(tableName.split("_"))
    if ( len(elements) >= 2 ):
        element = elements[-2].replace("List", "")
    else:
        element = ""
    return element


def buildBasePipeline(name, parentName=None):
    unwind = { "$unwind": { "path": "$" + name, "preserveNullAndEmptyArrays": False } }
    if (parentName):
        addFields = { "$addFields": { name + "." + parentName + "_id": "$_id" } }
    else:
        addFields = { "$addFields": { name + ".pid": "$" + ID_NAME} }
    replaceRoot = { "$replaceRoot": { "newRoot": "$" + name } }
    project = { "$project": { name: 0 }}
    return [unwind, addFields, replaceRoot, project]

def buildPipeline(name, level):
    pipeline = []
    for _ in range(level):
        pipeline.append({ "$unwind": { "path": "$" + name, "preserveNullAndEmptyArrays": False } })
        pipeline.append({ "$replaceRoot": { "newRoot": "$" + name } } )
    pipeline.extend(buildBasePipeline(name))
    return pipeline
    
def buildUnionStage(name, level):
    internalPipeline = buildPipeline (name, level)
    stage = { "$unionWith": { "coll": COLL_NAME, "pipeline": internalPipeline } }
    return stage

def replacePipeline(name, info):
    table = info["table"]
    parentName = getDocumentParentClass(table["table"])
    table["table"] = getDocumentClass(name)
    newPipeline = buildBasePipeline(name, parentName)
    for i in range(1, info["c"]):
        newPipeline.append(buildUnionStage(name, i))
    table["pipeline"] = newPipeline

def replaceColumns(name, info):
    table = info["table"]
    c = info["c"]
    colNameRegex = r"(" + name + r"\.){" + str(c) + r"}"
    newColumns = []
    for column in table["columns"]:
        if "_idx" in column["Name"]:
            continue
        column["Name"] = re.sub(colNameRegex, "", column["Name"])
        column["SqlName"] = re.sub(colNameRegex, "", column["SqlName"])
        newColumns.append(column)
    newColumns.append({"MongoType": "bson.ObjectId", "Name": "pid", "SqlName": "pid", "SqlType": "objectid"})
    parentName = getDocumentParentClass(table["table"])
    if (parentName):
        parentFieldName = parentName + "_id"
        newColumns.append({"MongoType": "bson.ObjectId", "Name": parentFieldName, "SqlName": parentFieldName, "SqlType": "objectid"})
    table["columns"] = newColumns

def isRecursiveSubClass(name, table):
    recursiveSubClassRegex = r".*(_" + name + ")+$"
    tableName = table["table"]
    return re.match(recursiveSubClassRegex, tableName)

def removeTables(name, db):
    db["tables"][:] = [ table for table in db["tables"] if not isRecursiveSubClass(name, table)]


with open(SRC_FILENAME, 'r') as stream:
    recursiveNameRegex = r"(^|_)([a-zA-Z0-9]+)_\2(_|$)"
    for schemas in yaml.load_all(stream):
        for schema, dbs in schemas.items():
            for db in dbs:
                recursiveClasses = {}
                tableIndex = {}
                for table in db["tables"]:
                    tableName = table["table"]
                    className = getDocumentClass(tableName)
                    if className in tableIndex:
                        tableIndex[className].append(tableName)
                    else:
                        tableIndex[className] = [tableName]
                    matches = re.findall(recursiveNameRegex, tableName)
                    if (matches):
                        recursiveClass = matches[0][1]
                        c = tableName.count(recursiveClass)
                        if recursiveClass in recursiveClasses:
                            currentMax = recursiveClasses[recursiveClass]
                        else:
                            currentMax = {"c": 0}
                        if (c > currentMax["c"]):
                            recursiveClasses[recursiveClass] = {"c": c, "tableName": tableName, "table": table}
                for recursiveClass, info in recursiveClasses.items():
                    #print(recursiveClass, "in", info["tableName"])
                    replaceColumns(recursiveClass, info)
                    replacePipeline(recursiveClass, info)
                    removeTables(recursiveClass, db)
        with open(DST_FILENAME, "w") as outFile:
            outFile.write(yaml.dump(schemas, default_flow_style=False))
