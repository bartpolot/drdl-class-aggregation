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

def getParentObjectName(tableName):
    objectNames = tableName.split("_")
    if (len(objectNames) < 2):
        return None
    if (objectNames[-2] == objectNames[-1]):
        return "pid"
    return objectNames[-2].replace("List", "") + "_id"

def addParentColumn(columnIndex, table):
    parentName = getParentObjectName(table["table"])
    if (parentName and parentName not in columnIndex):
        columnIndex[parentName] = {"MongoType": "bson.ObjectId", "Name": parentName, "SqlName": parentName, "SqlType": "objectid"}

def buildColumns(name, tables):
    """ Aggregate the columns from all tables that contain this class except idx columns """
    columnIndex = {}
    for table in tables:
        for column in [column for column in table["columns"] if "idx" not in column["Name"]]:
            columnName = columnNameCleanup(name, column["Name"])
            columnIndex[columnName] = column
            column["Name"] = columnName
            column["SqlName"] = columnName
        addParentColumn(columnIndex, table)
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

def buildPipeline(name, tables):
    """ Builds a pipeline to collect instances of a class from all possible nesting levels """
    initialStages = tables[0]["table"].split("_")
    if (len(initialStages) < 2):
        return []
    pipeline = buildGenericPipeline (name, initialStages)
    collectionName = tables[0]["collection"]
    for table in tables[1:]:
        stages = table["table"].split("_")
        if(len(stages) < 2):
            continue
        pipeline.append(buildUnionStage(name, stages, collectionName))
    return pipeline

################ Table ################

def buildClassTable(name, tables):
    """ Builds a Table definition for a class

    The table definition will contain:
    - The table name
    - The collection where the class is stored
    - The columns of this table
    - The aggregation pipeline needed to generate this table """
    dst = {"table": name, "collection": tables[0]["collection"]}
    dst["columns"] = buildColumns(name, tables)
    dst["pipeline"] = buildPipeline(name, tables)
    return dst

 ################ Classes ################

def getDocumentClassName(tableName):
    return tableName.split("_")[-1].replace("List", "")

def buildClassIndex(db):
    """ Build a dictionary of all classes in the database

    The dictionary contains each class as key and as value
    an array of all tables that contain this class """
    classIndex = {}
    for table in db["tables"]:
        className = getDocumentClassName(table["table"])
        if className in classIndex:
            classIndex[className].append(table)
        else:
            classIndex[className] = [table]
    return classIndex

################ Main ################

with open(SRC_FILENAME, 'r') as stream:
    for schemas in yaml.load_all(stream):
        for schema, dbs in schemas.items():
            for db in dbs:
                classIndex = buildClassIndex(db)
                unrolledTables = []
                for className in classIndex:
                    unrolledTables.append(buildClassTable(className, classIndex[className]))
                db["tables"] = unrolledTables
        with open(DST_FILENAME, "w") as outFile:
            outFile.write(yaml.dump(schemas, default_flow_style=False))
