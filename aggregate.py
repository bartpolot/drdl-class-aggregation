#!/usr/bin/env python3
#
# PoC code: no warranty, might explode, harmful to children, etc
#
# File structure of a DRDL:
# schema:
# - db: DB_NAME
#   tables:
#   - table: TABLE_NAME
#     collection: COLLECTION_NAME
#     pipeline:
#     - $stage1: [...]
#     - $stage2: [...]
#     columns: [{ MongoType, Name, SqlName, SqlType }]
#
# TODO:
# - optimize the unwind-replaceRoot steps with just one replaceRoot at the end
# - remove these constants:
ID_NAME = "oid"
SRC_FILENAME = "src.drdl"
DST_FILENAME = "dst.drdl"
# END TODO

import yaml
import re

################ Columns ################

def columnNameCleanup(className, columnName):
    """ Remove all prefixes and the List suffix from a column name 

    Example: coverageList.coverageList.coverageCode -> coverageCode"""
    colNameRegex = r".*" + className + r"(|List)\."
    return re.sub(colNameRegex, "", columnName)

def getParentObjectName(classPath):
    if (len(classPath) < 2):
        return None
    if (classPath[-2] == classPath[-1]):
        return "pid"
    return classPath[-2].replace("List", "") + "_id"

def addParentColumn(columnIndex, classPath):
    parentName = getParentObjectName(classPath)
    if (parentName and parentName not in columnIndex):
        columnIndex[parentName] = {"MongoType": "bson.ObjectId", "Name": parentName, "SqlName": parentName, "SqlType": "objectid"}

def buildColumns(classInfo, idx):
    """ Aggregate the columns from all tables that contain this class except idx columns 

    classInfo: [{ 'className': className, 'table': table, 'classPath': ['path', 'to', 'class']] }] """
    columnIndex = {}
    className = classInfo[0]["className"]
    print(className)
    for srcInfo in classInfo:
        table = srcInfo["table"]
        print(" ", srcInfo["classPath"], " - ", srcInfo["table"]["table"])
        for column in [column for column in table["columns"] if "idx" not in column["Name"]]:
            columnName = columnNameCleanup(srcInfo["className"], column["Name"])
            if columnName == "_id":
                print("      _id is not allowed in nested objects")
                continue
            if className not in column["Name"]: # this a field of the parent
                print("     ", column["Name"], "belongs to the parent class")
                continue
            if columnName in columnIndex: # already have it from other tables
                print("     ", columnName ,"- we've already seen this")
                continue
            if columnName.split(".")[0] in idx: # this is a full-time class
                print("     ", columnName.split(".")[0], "belongs in a separate table")
                continue
            print("   ", columnName, " <-- ", column["Name"])
            newColumn = dict(column)
            newColumn["Name"] = columnName
            newColumn["SqlName"] = columnName
            columnIndex[columnName] = newColumn
        addParentColumn(columnIndex, srcInfo["classPath"])
    return list(columnIndex.values())

################ Pipeline ################

def buildBasePipeline(stage, parentStage, rootStage):
    """ Builds a base 4 stage pipeline to extract a child class from a parent injecting the parent id """
    unwind = { "$unwind": { "path": "$" + stage, "preserveNullAndEmptyArrays": False } }
    parentName = parentStage.replace("List", "")
    if (parentStage == rootStage):
        addFields = { "$addFields": { stage + "." + parentName + "_id": "$_id" } }
    elif (parentStage != stage):
        addFields = { "$addFields": { stage + "." + parentName + "_id": "$" + ID_NAME } }
    else:
        addFields = { "$addFields": { stage + ".pid": "$" + ID_NAME} }
    replaceRoot = { "$replaceRoot": { "newRoot": "$" + stage } }
    project = { "$project": { stage: 0 }}
    return [unwind, addFields, replaceRoot, project]

def buildGenericPipeline(name, nestingStages):
    """ Builds a generic pipeline with optionally a number of stages to skip all levels above parent """
    pipeline = []
    for stage in nestingStages[1:-1]:
        pipeline.append({ "$unwind": { "path": "$" + stage, "preserveNullAndEmptyArrays": False } })
        pipeline.append({ "$replaceRoot": { "newRoot": "$" + stage } } )
    pipeline.extend(buildBasePipeline(nestingStages[-1], nestingStages[-2], nestingStages[0]))
    return pipeline
    
def buildUnionStage(name, stages, collectionName):
    """ Builds a union stage, including the internal pipeline to generate the set to do the union with """
    internalPipeline = buildGenericPipeline (name, stages)
    stage = { "$unionWith": { "coll": collectionName, "pipeline": internalPipeline } }
    return stage

def buildPipeline(classInfo):
    """ Builds a pipeline to collect instances of a class from all possible nesting levels """
    collectionName = classInfo[0]["table"]["collection"]
    initialStages = classInfo[0]["classPath"]
    className = classInfo[0]["className"]
    if (len(initialStages) < 2):
        return []
    pipeline = buildGenericPipeline (className, initialStages)
    for srcInfo in classInfo[1:]:
        stages = srcInfo["classPath"]
        if(len(stages) < 2):
            continue
        pipeline.append(buildUnionStage(className, stages, collectionName))
    return pipeline

################ Table ################

def buildClassTable(classInfo, idx):
    """ Builds a Table definition for a class

    The table definition will contain:
    - The table name
    - The collection where the class is stored
    - The columns of this table
    - The aggregation pipeline needed to generate this table """
    dst = {"table": classInfo[0]["className"],
          "collection": classInfo[0]["table"]["collection"]}
    dst["columns"] = buildColumns(classInfo, idx)
    dst["pipeline"] = buildPipeline(classInfo)
    return dst

################ Classes ################

def printClassIndex(classIndex):
    """ debug function """
    print("Nested classes detected:")
    for name, infos in classIndex.items():
        print(" ",name)
        for i in infos:
            print("   ", i["classPath"])

def getDocumentClassNameFromTable(tableName):
    return tableName.split("_")[-1].replace("List", "")

def getDocumentClassNameFromColumn(columnName):
    return columnName.split(".")[-2].replace("List", "")

def buildClassPath(column, table):
    """ Given a table name (for the name of the root) and
        the column name of the oid field, 
        build the nesting path to the class """
    classPath = [table["table"].split("_")[0]]
    classPath.extend(column["Name"].split(".")[:-1])
    return classPath

def buildClassIndex(db):
    """ Build a dictionary of all nested classes in the database

    The dictionary contains each class as key and as value
    an array of all tables that contain this class with info about
    the nesting depth for the class.

    { 'className': className, 'table': table, 'classPath': ['path', 'to', 'class']] }
     """
    classIndex = {}
    for table in db["tables"]:
        for column in [column for column in table["columns"] if ID_NAME in column["Name"]]:
            className = getDocumentClassNameFromColumn(column["Name"])
            srcInfo = { 'className': className, 
                        'table': table,
                        'classPath': buildClassPath(column, table) }
            if className in classIndex:
                classIndex[className].append(srcInfo)
            else:
                classIndex[className] = [srcInfo]
    printClassIndex(classIndex)
    return classIndex

################ Main ################

with open(SRC_FILENAME, 'r') as stream:
    for schemas in yaml.load_all(stream):
        for schema, dbs in schemas.items():
            for db in dbs:
                classIndex = buildClassIndex(db)
                # the root table is not nested, must be added explicitly
                unrolledTables = [db["tables"][0]]
                for classInfo in classIndex.values():
                    unrolledTables.append(buildClassTable(classInfo, classIndex))
                db["tables"] = unrolledTables
        with open(DST_FILENAME, "w") as outFile:
            outFile.write(yaml.dump(schemas, default_flow_style=False))
