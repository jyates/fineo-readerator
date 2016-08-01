# TODO

3. Reading
  a. filter condition rewrite based on alias names
  e. Better timestamp support - right now just UNIX epoch timestamp specification
    i. intervals
      I. Especially important in Dynamo to make sure that we do reads 'well' and avoid too much overhead (e.g. smart when bounding conditions on ts)
    ii. natural timestamp
    iii. timezones

3. Server
  a. stand up
  b. filter per-tenant based on properties
  c. load testing

--- Later ---
0. RecombinatorRecordBatch
 a. Improve alias to known field mapping to avoid copying values and instead use transfer pairs, 
 but requires using some sort of splitting, which is not well defined in drill right now

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
