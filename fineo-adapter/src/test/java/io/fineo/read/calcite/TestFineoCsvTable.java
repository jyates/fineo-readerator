package io.fineo.read.calcite;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.schemarepo.ValidatorFactory;

import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static java.lang.String.format;

/**
 * Read static CSV data from resources/csv-test/TABLE_1.csv
 */
public class TestFineoCsvTable extends BaseDynamoTableTest {

  @Test
  public void testSelectStar() throws Exception {
    // setup the schema repository
    DynamoDBRepository repository =
      new DynamoDBRepository(ValidatorFactory.EMPTY, tables.getAsyncClient(),
        getCreateTable(tables.getTestTableName()));
    SchemaStore store = new SchemaStore(repository);

    // create a simple schema and store it
    String org = "orgid1", metrictype = "metricid1", fieldname = "field1";
    SchemaTestUtils.addNewOrg(store, org, metrictype, fieldname);

    // read the CSV data through calcite.
    LocalDynamoTestUtil util = dynamo.getUtil();
    CalciteAssert.that()
                 .with(new TableModelBuilder()
                   .setDynamo(util.getUrl())
                   .useCsv()
                   .setSchemaTable(tables.getTestTableName())
                   .build())
                 // This works...weird. But the other lookup doesn't
//                 .query("select * from \"events\"")
//                 .returnsCount(1);
                 .query(format("select * from \"events\" WHERE " +
                        ORG_ID_KEY + " = '%s' AND " +
                        ORG_METRIC_TYPE_KEY + " = '%s'", org, metrictype))
                 .returnsCount(1);
  }

  private CreateTableRequest getCreateTable(String schemaTable) {
    CreateTableRequest create =
      DynamoDBRepository.getBaseTableCreate(schemaTable);
    create.setProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(1L)
      .withWriteCapacityUnits(1L));
    return create;
  }
}
