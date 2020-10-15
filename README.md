# drdl-class-aggregation

By default MongoDB's BI Connector doesn't cope well with nested objects.
The default DRDL creates one table for each object class and nesting level. 
If object from a class are found in three different nesting levels, there will be three tables with objects of this class.
This is particularly problematic with recursive objects that can be nested indefinitely.

This script modifies the default DRDL to collect all objects of one class in one table, regardless of where they are.

Extract the default drdl with mongodrdl, apply this script to modify it and stard mongosqld using the modified drdl as schema.
