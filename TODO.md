# TODO

3. Reading
  a. support time-based decision making for reads as a directory/table filter
    i. pull out time queries and join together into a coherent filter on time
    ii. support dynamo table-name partitioning
  
2. Dynamo Reading
 1. fix reading Decimal38 (now just reads strings)
 2. support transfer pairs for base fields, and fields that only have a single value

3. Server
  a. stand up
  b. integration with dynamo + spark
  c. load testing


--- Later ---
1. Spark Reading
  0. integrate into drill.
  a. mapping canonical fields to query fields (canonical + aliases)
  b. mapping results back to canonical fields
  c. integrating into calcite
