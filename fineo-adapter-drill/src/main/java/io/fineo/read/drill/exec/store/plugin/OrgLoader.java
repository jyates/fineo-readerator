package io.fineo.read.drill.exec.store.plugin;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.common.base.Preconditions;
import io.fineo.user.info.DynamoTenantInfoStore;
import io.fineo.user.info.TenantInfoStore;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

/**
 * Encapsulate how we get the org ids for the schema factory.
 */
public class OrgLoader implements Iterable<String> {

  private final Supplier<AmazonDynamoDBAsyncClient> client;
  private List<String> hardCodedOrgs;
  private String dynamoOrgTable;

  public OrgLoader(List<String> hardCodedOrgs, String dynamoOrgTable,
    Supplier<AmazonDynamoDBAsyncClient> dynamoClient) {
    this.hardCodedOrgs = hardCodedOrgs;
    this.dynamoOrgTable = dynamoOrgTable;
    this.client = dynamoClient;
  }

  @Override
  public Iterator<String> iterator() {
    if (this.hardCodedOrgs != null && this.hardCodedOrgs.size() > 0) {
      return this.hardCodedOrgs.iterator();
    }

    Preconditions.checkNotNull(this.dynamoOrgTable, "No dynamo tenant table specified!");
    TenantInfoStore tenants = new DynamoTenantInfoStore(client.get(), this.dynamoOrgTable, null);
    return StreamSupport.stream(tenants.getTenants().spliterator(), true)
                        .map(info -> info.getApiKey()).iterator();
  }
}
