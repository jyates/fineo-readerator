package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.RepeatedBitVector;
import org.apache.drill.exec.vector.RepeatedDecimal38SparseVector;
import org.apache.drill.exec.vector.RepeatedVarBinaryVector;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VectorDescriptor;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.System.arraycopy;
import static org.apache.drill.common.types.TypeProtos.MajorType;
import static org.apache.drill.common.types.TypeProtos.MinorType;
import static org.apache.drill.common.types.TypeProtos.MinorType.VARCHAR;

/**
 * Actually do the get/query/scan based on the {@link DynamoSubScan.DynamoSubScanSpec}.
 */
public class DynamoRecordReader extends AbstractRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoRecordReader.class);
  private static final Joiner DOTS = Joiner.on('.');
  private static final Joiner COMMAS = Joiner.on(", ");

  private final AWSCredentialsProvider credentials;
  private final ClientConfiguration clientConf;
  private final DynamoSubScan.DynamoSubScanSpec scanSpec;
  private final boolean consistentRead;
  private final DynamoEndpoint endpoint;
  private OutputMutator outputMutator;
  private OperatorContext operatorContext;
  private AmazonDynamoDBAsyncClient client;
  private Iterator<Page<Item, ScanOutcome>> resultIter;
  private Map<String, Pair<ValueVector, TypeProtos.MajorType>> scalars = new HashMap<>();
  private Map<String, MapVector> mapVectors = new HashMap<>();
  private Map<String, Pair<ListVector, ValueVector>> listVectors = new HashMap<>();

  public DynamoRecordReader(AWSCredentialsProvider credentials, ClientConfiguration clientConf,
    DynamoEndpoint endpoint, DynamoSubScan.DynamoSubScanSpec scanSpec,
    List<SchemaPath> columns, boolean consistentRead) {
    this.credentials = credentials;
    this.clientConf = clientConf;
    this.scanSpec = scanSpec;
    this.consistentRead = consistentRead;
    this.endpoint = endpoint;
    setColumns(columns);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = context;
    this.outputMutator = output;

    this.client = new AmazonDynamoDBAsyncClient(credentials, this.clientConf);
    endpoint.configure(this.client);

    // setup the vectors that we know we will need - the primary key(s)
    Map<String, String> pkToType = scanSpec.getTable().getPkToType();
    for (Map.Entry<String, String> pk : pkToType.entrySet()) {
      // pk has to be a scalar type, so we never get null here
      MinorType type = translateDynamoToDrillType(pk.getValue()).minor;
      MajorType mt = Types.required(type);
      MaterializedField field = MaterializedField.create(pk.getKey(), mt);
      ValueVector vv = TypeHelper.getNewVector(field, operatorContext.getAllocator(), outputMutator
        .getCallBack());
      scalars.put(pk.getKey(), new ImmutablePair<>(vv, mt));
    }

    Table table = new DynamoDB(client).getTable(scanSpec.getTable().getName());
    ScanSpec spec = new ScanSpec();
    spec.withConsistentRead(consistentRead);
    // basic scan requirements
    int limit = scanSpec.getLimit();
    if (limit > 0) {
      spec.setMaxPageSize(limit);
    }
    spec.withSegment(scanSpec.getSegmentId()).withTotalSegments(scanSpec.getTotalSegments());

    // TODO skip queries, which just want the primary key values
    // TODO support repeat field projections, e.g. *, field1
    // projections
    if (!isStarQuery()) {
      List<String> columns = new ArrayList<>();
      for (SchemaPath column : getColumns()) {
        // build the full name of the column
        List<String> parts = new ArrayList<>();
        PathSegment.NameSegment name = column.getRootSegment();
        PathSegment seg = name;
        parts.add(name.getPath());
        while ((seg = seg.getChild()) != null) {
          if (seg.isArray()) {
            parts
              .add(parts.remove(parts.size() - 1) + "[" + seg.getArraySegment().getIndex() + "]");
          } else {
            parts.add(seg.getNameSegment().getPath());
          }
        }
        columns.add(DOTS.join(parts));
      }
      spec.withProjectionExpression(COMMAS.join(columns));
    }

    ItemCollection<ScanOutcome> results = table.scan(spec);
    this.resultIter = results.pages().iterator();
  }

  private MajorOrMinor translateDynamoToDrillType(String type) {
    switch (type) {
      // Scalar types
      case "S":
        return new MajorOrMinor(VARCHAR);
      case "N":
        return new MajorOrMinor(MinorType.DECIMAL38SPARSE);
      case "B":
        return new MajorOrMinor(MinorType.VARBINARY);
      case "BOOL":
        return new MajorOrMinor(MinorType.BIT);
      case "SS":
        return new MajorOrMinor(Types.repeated(translateDynamoToDrillType("S").minor));
      case "NS":
        new MajorOrMinor(Types.repeated(translateDynamoToDrillType("N").minor));
      case "BS":
        new MajorOrMinor(Types.repeated(translateDynamoToDrillType("B").minor));
      case "M":
        return new MajorOrMinor(MinorType.MAP);
      case "L":
        return new MajorOrMinor(MinorType.LIST);
    }
    throw new IllegalArgumentException("Don't know how to translate type: " + type);
  }

  private class MajorOrMinor {
    private MajorType major;
    private MinorType minor;

    public MajorOrMinor(MajorType major) {
      this.major = major;
    }

    public MajorOrMinor(MinorType minor) {
      this.minor = minor;
    }
  }

  @Override
  public int next() {
    Stopwatch watch = Stopwatch.createStarted();

    scalars.values().stream().map(p -> p.getKey()).forEach(vv -> vv.clear());
    mapVectors.values().stream().forEach(map -> map.clear());
    listVectors.values().stream().forEach(pair -> {
      pair.getKey().clear();
      pair.getValue().clear();
    });

    int count = 0;
    Page<Item, ScanOutcome> page;
    while (resultIter.hasNext()) {
      page = resultIter.next();
      for (Item item : page) {
        int rowCount = count++;
        for (Map.Entry<String, Object> attribute : item.attributes()) {
          String name = attribute.getKey();
          Object value = attribute.getValue();
          // special handling for non-scalar types
          if (value instanceof Map) {
            MapVector map = mapVectors.get(name);
            if (map == null) {
              MaterializedField field =
                MaterializedField.create(attribute.getKey(), Types.optional(MinorType.MAP));
              map = getField(field, MapVector.class);
              mapVectors.put(name, map);
            }
            addToMap(rowCount, map, (Map<String, Object>) value);
          } else if (value instanceof List || value instanceof Set) {
            // sets and lists handled the same - as a repeated list of values
            Pair<ListVector, ValueVector> list = listVectors.get(name);
            if (list == null) {
              MaterializedField field =
                MaterializedField.create(name, Types.optional(MinorType.LIST));
              list = new MutablePair<>(getField(field, ListVector.class), null);
              list.getKey().allocateNew();
              listVectors.put(name, list);
            }
            addToList(list, ((Collection<Object>) value), rowCount);
          } else {
            writeScalar(rowCount, name, value);
          }
        }
      }
    }

    LOG.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), count);
    return count;
  }

  private void addToList(Pair<ListVector, ValueVector> pair, Iterable<Object> values, int index) {
    ListVector list = pair.getKey();
//    list.getWriter().bit().writeBit(1);
//    list.getWriter().bit().writeBit(1);
    UInt4Vector offsets = list.getOffsetVector();
    int nextOffset = offsets.getAccessor().get(index);
    list.getMutator().setNotNull(index);

    Object first = values.iterator().next();
    MinorType minor = getMinorType(first);
    MajorType major = Types.optional(minor);

    ValueVector vector = pair.getValue();
    if (vector == null) {
      // create the 'writer' vector target
      vector = list.addOrGetVector(new VectorDescriptor(major)).getVector();
      vector.allocateNew();
      pair.setValue(vector);
    }

    int listIndex = 0;
    for (Object val : values) {
      writeScalar(nextOffset + listIndex, val, vector, major);
      listIndex++;
    }
    offsets.getMutator().setSafe(index + 1, nextOffset + listIndex);

  }

  private void addToMap(int index, MapVector map, Map<String, Object> values) {
    map.allocateNew();
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      String name = entry.getKey();
      Object value = entry.getValue();
      MinorType minor = getMinorType(value);
      MajorType major = Types.optional(minor);
      ValueVector vector =
        map.addOrGet(name, major, TypeHelper.getValueVectorClass(minor, major.getMode()));
      vector.allocateNew();
      writeScalar(index, value, vector, major);
    }
  }

  private void writeScalar(int index, String column, Object columnValue) {
    Pair<ValueVector, TypeProtos.MajorType> vector = this.scalars.get(column);
    ValueVector vv;
    if (vector == null) {
      MinorType minor = getMinorType(columnValue);
      MajorType type = Types.optional(minor);
      MaterializedField field = MaterializedField.create(column, type);
      Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(minor, type.getMode());
      vector = new ImmutablePair<>(getField(field, clazz), type);
      this.scalars.put(column, vector);
    }
    vv = vector.getKey();
    vv.allocateNew();
    writeScalar(index, columnValue, vv, vector.getValue());
  }

  private <T extends ValueVector> T getField(MaterializedField field, Class<T> clazz) {
    try {
      return outputMutator.addField(field, clazz);
    } catch (SchemaChangeException e) {
      throw new DrillRuntimeException("Failed to create vector for field: " + field.getName(), e);
    }
  }

  private void writeScalar(int index, Object value, ValueVector vector, MajorType
    type) {
    MinorType minor = type.getMinorType();
    type:
    switch (minor) {
      case VARCHAR:
        byte[] bytes = ((String) value).getBytes();
        switch (type.getMode()) {
          case OPTIONAL:
            ((NullableVarCharVector.Mutator) vector.getMutator()).setSafe(index, bytes,
              0, bytes.length);
            break type;
          case REQUIRED:
            ((VarCharVector.Mutator) vector.getMutator()).setSafe(index, bytes);
            break type;
          case REPEATED:
            ((RepeatedVarCharVector.Mutator) vector.getMutator()).addSafe(index, bytes);
            break type;
          default:
            failForMode(type);
        }
      case BIT:
        int bool = ((Boolean) value).booleanValue() ? 1 : 0;
        switch (type.getMode()) {
          case OPTIONAL:
            NullableBitVector bv = (NullableBitVector) vector;
            bv.getMutator().setSafe(index, bool);
            bv.getMutator().setValueCount(bv.getAccessor().getValueCount() + 1);
            break type;
          case REQUIRED:
            ((BitVector.Mutator) vector.getMutator()).setSafe(index, bool);
            break type;
          case REPEATED:
            ((RepeatedBitVector.Mutator) vector.getMutator()).addSafe(index, bool);
            break type;
          default:
            failForMode(type);
        }
      case DECIMAL38SPARSE:
        BigDecimal decimal = (BigDecimal) value;
        BigInteger intVal = decimal.unscaledValue();
        byte[] sparseInt = new byte[24];
        byte[] intBytes = intVal.toByteArray();
        arraycopy(intBytes, 0, sparseInt, sparseInt.length - intBytes.length, intBytes.length);
        // TODO revisit handling of Decimal38Dense
        // kind of an ugly way to manage the actual transfer of bytes. However, this is much
        // easier than trying to manage a larger page of bytes.
        Decimal38SparseHolder holder = new Decimal38SparseHolder();
        holder.start = 0;
        holder.buffer = operatorContext.getManagedBuffer(sparseInt.length);
        holder.buffer.setBytes(0, sparseInt);
        holder.precision = decimal.precision();
        holder.scale = decimal.scale();
        switch (type.getMode()) {
          case OPTIONAL:
            ((NullableDecimal38SparseVector.Mutator) vector.getMutator())
              .setSafe(index, holder);
            break type;
          case REQUIRED:
            ((Decimal38SparseVector.Mutator) vector.getMutator()).setSafe(index, holder);
            break type;
          case REPEATED:
            ((RepeatedDecimal38SparseVector.Mutator) vector.getMutator()).addSafe(index, holder);
            break type;
          default:
            failForMode(type);
        }
      case VARBINARY:
        byte[] bytesBinary = (byte[]) value;
        switch (type.getMode()) {
          case OPTIONAL:
            ((NullableVarBinaryVector.Mutator) vector.getMutator()).setSafe(index,
              bytesBinary, 0, bytesBinary.length);
            break type;
          case REQUIRED:
            ((VarBinaryVector.Mutator) vector.getMutator()).setSafe(index, bytesBinary);
            break type;
          case REPEATED:
            ((RepeatedVarBinaryVector.Mutator) vector.getMutator()).addSafe(index, bytesBinary);
            break type;
          default:
            failForMode(type);
        }
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  private void failForMode(MajorType type) {
    throw new IllegalArgumentException(
      "Mode: " + type.getMode() + " not " + "supported for scalar type: " + type);
  }

  private MinorType getMinorType(Object value) {
    if (value instanceof String) {
      return VARCHAR;
    } else if (value instanceof Boolean) {
      return MinorType.BIT;
    } else if (value instanceof BigDecimal) {
      return MinorType.DECIMAL38SPARSE;
    } else if (value instanceof byte[]) {
      return MinorType.VARBINARY;
    }
    throw new UnsupportedOperationException("Unexpected type for: " + value);
  }

  @Override
  public void close() throws Exception {
    this.client.shutdown();
  }
}
