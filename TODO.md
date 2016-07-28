# TODO

3. Reading
  a. Verify timerange source filtering for dynamo reads (limit(0) support)
  b. Compound key support for dynamo reads
  c. Filter sources based on timerange (dynamo as much as possible, fs otherwise)
  d. Merge dynamo and fs reads into a single read path
  e. Better timestamp support - right now just UNIX epoch timestamp specification
    i. intervals
    ii. natural timestamp
    iii. timezones

3. Server
  a. stand up
  b. integration with dynamo + spark
  c. load testing

--- Later ---
0. RecombinatorRecordBatch
 a. Improve alias to known field mapping to avoid copying values and instead use transfer pairs

2. Dynamo Reading
 a. fix reading Decimal38 (now just reads strings)
 b. Better support for PK Mapping.
  i. Separate sort and hash key expansion in mapper so we can be smart about what fields to 
  include in the table definition
  ii. Use the KeyMapper when building the filter condition so we can avoid referencing the 
  'actual' fields at all 

3. Drill
  a. Support _fm field by enabling merging schemas in ExternalSort/TopN. 
    ii. Currently only supports 'union' logic and merging schemas for non-complex types. We enable the union logic, but only use the schema merging support. This same logic can be applied to complex types (map/list) so we can read those as well.

4. Timerange pushdown
 a. support tracking the last json -> parquet conversion time so we create several different plans (progressively removing dynamo tables and replacing them with parquet scans)

1. Spark Reading
  0. integrate into drill.
  a. mapping canonical fields to query fields (canonical + aliases)
  b. mapping results back to canonical fields
  c. integrating into calcite
