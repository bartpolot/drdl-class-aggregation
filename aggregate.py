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
# END TODO

import yaml
import re
import logging
import argparse

################ Defaults ################

ID_NAME = "oid"
SRC_FILENAME = "src.drdl"
DST_FILENAME = "dst.drdl"

################ Globals ################

idName = ID_NAME

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
    logging.info("  %s", className)
    for srcInfo in classInfo:
        table = srcInfo["table"]
        logging.debug("  %s - %s", srcInfo["classPath"], srcInfo["table"]["table"])
        for column in [column for column in table["columns"] if "idx" not in column["Name"]]:
            columnName = columnNameCleanup(srcInfo["className"], column["Name"])
            if columnName == "_id":
                logging.debug("    _id is not allowed in nested objects")
                continue
            if className not in column["Name"]: # this a field of the parent
                logging.debug("    %s belongs to the parent class", column["Name"])
                continue
            if columnName in columnIndex: # already have it from other tables
                logging.debug("    %s - we've already seen this", columnName)
                continue
            if columnName.split(".")[0] in idx: # this is a full-time class
                logging.debug("    %s belongs in a separate table", columnName.split(".")[0])
                continue
            logging.info("    %s <-- %s", columnName, column["Name"])
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
        addFields = { "$addFields": { stage + "." + parentName + "_id": "$" + idName } }
    else:
        addFields = { "$addFields": { stage + ".pid": "$" + idName} }
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
    logging.info("Nested classes detected:")
    for name, infos in classIndex.items():
        logging.info("  %s",name)
        for i in infos:
            logging.debug("    %s", i["classPath"])

def getDocumentClassNameFromTable(tableName):
    return tableName.split("_")[-1].replace("List", "")

def getDocumentClassNameFromColumn(columnName):
    try:
        return columnName.split(".")[-2].replace("List", "")
    except IndexError:
        raise Exception("Class marker %s not found in column name %s" % (idName, columnName)) from None
    

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
        idRegex = r".*\." + idName + r"$"
        for column in [column for column in table["columns"] if re.match(idRegex, column["Name"]) ]:
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

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--inputfile", type=argparse.FileType('r'), default=SRC_FILENAME,
                    help="filename for the input DRDL")
parser.add_argument("-o", "--outputfile", type=argparse.FileType('w'), default=DST_FILENAME,
                    help="filename for the output DRDL")
parser.add_argument("-c", "--class-marker", type=str, default=ID_NAME,
                    help="fields with this name are detected as classes, defaults to " + ID_NAME)
parser.add_argument("-l", "--log", type=str, default="logging.INFO",
                    help="logging level, defaults to INFO")
args = parser.parse_args()
loggingLevel = getattr(logging, args.log.upper(), logging.INFO)
logging.basicConfig(format='%(message)s', level=loggingLevel)
idName = args.class_marker
print(idName)
with open(SRC_FILENAME, 'r') as stream:
    for schemas in yaml.safe_load_all(stream):
        for schema, dbs in schemas.items():
            for db in dbs:
                classIndex = buildClassIndex(db)
                # the root table is not nested, must be added explicitly
                unrolledTables = [db["tables"][0]]
                logging.info("Schema:")
                for classInfo in classIndex.values():
                    unrolledTables.append(buildClassTable(classInfo, classIndex))
                db["tables"] = unrolledTables
        with open(DST_FILENAME, "w") as outFile:
            outFile.write(yaml.dump(schemas, default_flow_style=False))
