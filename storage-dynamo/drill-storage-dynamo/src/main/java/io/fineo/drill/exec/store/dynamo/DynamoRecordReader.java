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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.AddOrGetResult;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.collect.Lists.newArrayList;
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
  private final Multimap<String, Integer> listIndexes = ArrayListMultimap.create();

  private Map<String, ScalarVectorStruct> scalars = new HashMap<>();
  private Map<String, MapVectorStruct> mapVectors = new HashMap<>();
  private Map<String, ListVectorStruct> listVectors = new HashMap<>();

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
      MinorType minor = translateDynamoToDrillType(pk.getValue()).minor;
      MajorType major = Types.required(minor);
      String name = pk.getKey();
      MaterializedField field = MaterializedField.create(name, major);
      Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(minor, major.getMode());
      ValueVector vv = getField(field, clazz);
      scalars.put(name, new ScalarVectorStruct(vv, major));
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
            int index = seg.getArraySegment().getIndex();
            String fullListName = DOTS.join(parts);
            String listLeafName = parts.remove(parts.size() - 1);
            parts.add(listLeafName + "[" + index + "]");
            listIndexes.put(fullListName, index);
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
    // reset all the vectors and prepare them for writing
    scalars.values().stream().forEach(struct -> struct.reset());
    mapVectors.values().stream().forEach(map -> map.reset());
    listVectors.values().stream().forEach(list -> list.reset());

    int count = 0;
    Page<Item, ScanOutcome> page;
    while (resultIter.hasNext()) {
      page = resultIter.next();
      for (Item item : page) {
        int rowCount = count++;
        for (Map.Entry<String, Object> attribute : item.attributes()) {
          String name = attribute.getKey();
          Object value = attribute.getValue();
          handleField(rowCount, name, value, scalars, mapVectors, listVectors, this::getField);
        }
      }
    }

    int rows = count;
    scalars.values().stream().forEach(scalar -> scalar.setValueCount(rows));
    mapVectors.values().stream().forEach(map -> map.setValueCount(rows));
    listVectors.values().stream().forEach(list -> list.setValueCount(rows));

    LOG.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), count);
    return count;
  }

  private void handleField(int rowCount, String name, Object value,
    Map<String, ScalarVectorStruct> scalars,
    Map<String, MapVectorStruct> mapVectors, Map<String, ListVectorStruct> listVectors,
    BiFunction<MaterializedField, Class<? extends ValueVector>, ValueVector> vectorFunc) {
    // special handling for non-scalar types
    if (value instanceof Map) {
      handleMap(rowCount, name, value, mapVectors, vectorFunc);
    } else if (value instanceof List || value instanceof Set) {
      handleList(rowCount, name, value, listVectors, vectorFunc);
    } else {
      writeScalar(rowCount, name, value, scalars, vectorFunc);
    }
  }

  private void handleMap(int row, String name, Object value,
    Map<String, MapVectorStruct> mapVectors,
    BiFunction<MaterializedField, Class<? extends ValueVector>, ValueVector> vectorFunc) {
    MapVectorStruct map = mapVectors.get(name);
    if (map == null) {
      MaterializedField field = MaterializedField.create(name, Types.optional(MinorType.MAP));
      MapVector vector = (MapVector) vectorFunc.apply(field, MapVector.class);
      vector.allocateNew();
      map = new MapVectorStruct(vector);
      mapVectors.put(name, map);
    }

    Map<String, Object> values = (Map<String, Object>) value;
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      String fieldName = entry.getKey();
      Object fieldValue = entry.getValue();
      handleField(row, fieldName, fieldValue, map.getScalarStructs(), map.getMapStructs(),
        map.getListStructs(), map.getVectorFun());
    }
    MapVector vector = map.getVector();
    vector.getMutator().setValueCount(vector.getAccessor().getValueCount() + 1);
  }

  /**
   * Sets and lists handled the same - as a repeated list of values
   *
   * @param index
   * @param name
   * @param value
   * @param listVectors
   */
  private void handleList(int index, String name, Object value,
    Map<String, ListVectorStruct> listVectors,
    BiFunction<MaterializedField, Class<? extends ValueVector>, ValueVector> vectorFun) {
    value = adjustListValueForPointSelections(name, value);

    // create the struct, if necessary
    ListVectorStruct struct = listVectors.get(name);
    if (struct == null) {
      MaterializedField field =
        MaterializedField.create(name, Types.optional(MinorType.LIST));
      ListVector listVector = (ListVector) vectorFun.apply(field, ListVector.class);
      listVector.allocateNew();
      struct = new ListVectorStruct(listVector);
      listVectors.put(name, struct);
    }

    // find the first item in the list
    List<Object> values;
    if (value instanceof List) {
      values = (List<Object>) value;
    } else {
      values = newArrayList((Collection<Object>) value);
    }
    Object first = values.iterator().next();
    MinorType minor = getMinorType(first);
    MajorType major = Types.optional(minor);

    // create the struct map based on the type. We might need to create the struct later, so
    // generate the right function call too
    Map map;
    Function<ValueVector, ? extends ValueVectorStruct> creator;
    boolean mapField = false, listField = false;
    switch (minor) {
      case MAP:
        map = struct.getMapStructs();
        creator = (vector) -> new MapVectorStruct((MapVector) vector);
        mapField = true;
        break;
      case LIST:
        map = struct.getListStructs();
        creator = (vector) -> new ListVectorStruct((ListVector) vector);
        listField = true;
        break;
      default:
        map = struct.getScalarStructs();
        creator = (vector) -> new ScalarVectorStruct(vector, major);
    }

    // there is only ever one vector that we use in a list, and its keyed by null
    ListVector list = struct.getVector();
    String vectorName = "_0list_name";
    ValueVectorStruct vectorStruct = (ValueVectorStruct) map.get(vectorName);
    if (vectorStruct == null) {
      // create the 'writer' vector target
      ValueVector vector = list.addOrGetVector(new VectorDescriptor(major)).getVector();
      vector.allocateNew();
      vectorStruct = creator.apply(vector);
      map.put(vectorName, vectorStruct);
    }

    UInt4Vector offsets = list.getOffsetVector();
    int nextOffset = offsets.getAccessor().get(index);
    list.getMutator().setNotNull(index);
    int listIndex = 0;
    Map<String, MapVectorStruct> maps = mapField ? map : null;
    Map<String, ListVectorStruct> lists = listField ? map : null;
    Map<String, ScalarVectorStruct> scalars = (!mapField && !listField) ? map : null;
    for (Object val : values) {
      // we should never need to create a sub-vector immediately - we created the vector above -
      // so send 'null' as the creator function.
      handleField(nextOffset + listIndex, vectorName, val, scalars, maps, lists, null);
      listIndex++;
    }
//    vector.getMutator().setValueCount(vector.getAccessor().getValueCount() + listIndex);
    offsets.getMutator().setSafe(index + 1, nextOffset + listIndex);
  }

  private Object adjustListValueForPointSelections(String name, Object value) {
    // Sometimes we might want to select an offset into a list. Dynamo handles this as returning
    // a smaller list. Drill expects the whole list to be there, so we need to fill in the other
    // parts of the list with nulls, up to the largest value
    Collection<Integer> indexes = listIndexes.get(name);
    if (indexes == null || indexes.size() == 0) {
      return value;
    }
    List<Integer> sorted = newArrayList(indexes);
    Collections.sort(sorted);
    int max = sorted.get(sorted.size() - 1);
    List<Object> actualValue = (List) value;
    List<Object> valueOut = new ArrayList<>(max);
    int last = 0;
    for (Integer index : sorted) {
      // fill in nulls between the requested indexes
      for (int i = last; i < index; i++) {
        valueOut.add(null);
      }
      valueOut.add(actualValue.remove(0));
      last++;
    }

    return valueOut;
  }

  private void writeScalar(int index, String column, Object columnValue, Map<String,
    ScalarVectorStruct> scalars, BiFunction<MaterializedField, Class<? extends ValueVector>,
    ValueVector> vectorFunc) {
    ScalarVectorStruct struct = scalars.get(column);
    if (struct == null) {
      MinorType minor = getMinorType(columnValue);
      MajorType type = Types.optional(minor);
      MaterializedField field = MaterializedField.create(column, type);
      Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(minor, type.getMode());
      ValueVector vv = vectorFunc.apply(field, clazz);
      vv.allocateNew();
      struct = new ScalarVectorStruct(vv, type);
      scalars.put(column, struct);
    }
    writeScalar(index, columnValue, struct);
  }

  private <T extends ValueVector> T getField(MaterializedField field, Class<T> clazz) {
    try {
      return outputMutator.addField(field, clazz);
    } catch (SchemaChangeException e) {
      throw new DrillRuntimeException("Failed to create vector for field: " + field.getName(), e);
    }
  }

  private void writeScalar(int index, Object value, ScalarVectorStruct struct) {
    if (value == null) {
      return;
    }
    MajorType type = struct.getType();
    MinorType minor = type.getMinorType();
    ValueVector vector = struct.getVector();
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
    } else if (value instanceof Map) {
      return MinorType.MAP;
    } else if (value instanceof List) {
      return MinorType.LIST;
    }
    throw new UnsupportedOperationException("Unexpected type for: " + value);
  }

  @Override
  public void close() throws Exception {
    this.client.shutdown();
  }

  // Struct for a logical ValueVector
  private abstract class ValueVectorStruct<T extends ValueVector> {
    private final T vector;

    public ValueVectorStruct(T vector) {
      this.vector = vector;
    }

    public T getVector() {
      return vector;
    }

    public void reset() {
      this.getVector().clear();
      this.getVector().allocateNew();
    }

    public void setValueCount(int valueCount) {
      this.getVector().getMutator().setValueCount(valueCount);
    }
  }

  private class ScalarVectorStruct extends ValueVectorStruct<ValueVector> {
    private final MajorType type;

    public ScalarVectorStruct(ValueVector vector, MajorType type) {
      super(vector);
      this.type = type;

    }

    public MajorType getType() {
      return type;
    }
  }

  private abstract class NestedVectorStruct<T extends ValueVector> extends ValueVectorStruct<T> {

    private final BiFunction<MaterializedField, Class<? extends ValueVector>, ValueVector>
      vectorFun;
    private Map<String, ScalarVectorStruct> scalarStructs = new HashMap<>();
    private Map<String, ListVectorStruct> listStructs = new HashMap<>();
    private Map<String, MapVectorStruct> mapStructs = new HashMap<>();

    public NestedVectorStruct(T vector, BiFunction<MaterializedField,
      Class<? extends ValueVector>, ValueVector> vectorFunc) {
      super(vector);
      this.vectorFun = vectorFunc;
    }

    public Map<String, ListVectorStruct> getListStructs() {
      return listStructs;
    }

    public Map<String, MapVectorStruct> getMapStructs() {
      return mapStructs;
    }

    public Map<String, ScalarVectorStruct> getScalarStructs() {
      return scalarStructs;
    }

    public BiFunction<MaterializedField, Class<? extends ValueVector>, ValueVector> getVectorFun() {
      return vectorFun;
    }

    public void clear() {
      this.getVector().clear();
    }

    @Override
    public void reset() {
      super.reset();
      this.listStructs.values().forEach(s -> s.clear());
      this.listStructs.clear();
      this.mapStructs.values().forEach(m -> m.clear());
      this.mapStructs.clear();
      this.scalarStructs.values().forEach(s -> s.getVector().clear());
    }
  }

  private class MapVectorStruct extends NestedVectorStruct<MapVector> {

    public MapVectorStruct(MapVector vector) {
      super(vector, (field, clazz) -> vector.addOrGet(field.getName(), field.getType(), clazz));
    }
  }

  private class ListVectorStruct extends NestedVectorStruct<ListVector> {

    public ListVectorStruct(final ListVector vector) {
      super(vector, (field, clazz) -> {
        AddOrGetResult result = vector.addOrGetVector(new VectorDescriptor(field.getType
          ()));
        return result.getVector();
      });
    }
  }
}
