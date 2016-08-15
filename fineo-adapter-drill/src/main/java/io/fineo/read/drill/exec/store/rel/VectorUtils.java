package io.fineo.read.drill.exec.store.rel;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

/**
 *
 */
public class VectorUtils {

  private VectorUtils() {
  }

  public static void write(String outputName, VectorWrapper wrapper, BaseWriter.MapWriter writer,
    int index) {
    write(outputName, wrapper.getValueVector(), wrapper.getField().getType().getMinorType(), writer,
      index, index);
  }

  public static void write(String name, VectorWrapper wrapper, BaseWriter.MapWriter writer,
    int inIndex, int outIndex) {
    write(name, wrapper.getValueVector(), wrapper.getField().getType().getMinorType(), writer,
      inIndex, outIndex);
  }

  public static void write(String outputName, ValueVector vector, TypeProtos.MinorType type,
    BaseWriter.MapWriter writer, int inIndex, int outIndex) {
    FieldReader reader = vector.getReader();
    reader.setPosition(inIndex);
    writer.setPosition(outIndex);
    switch (type) {
      case VARCHAR:
      case FIXEDCHAR:
        reader.copyAsValue(writer.varChar(outputName));
        break;
      case FLOAT4:
        reader.copyAsValue(writer.float4(outputName));
        break;
      case FLOAT8:
        reader.copyAsValue(writer.float8(outputName));
        break;
      case VAR16CHAR:
      case FIXED16CHAR:
        reader.copyAsValue(writer.var16Char(outputName));
        break;
      case INT:
        reader.copyAsValue(writer.integer(outputName));
        break;
      case SMALLINT:
        reader.copyAsValue(writer.smallInt(outputName));
        break;
      case TINYINT:
        reader.copyAsValue(writer.tinyInt(outputName));
        break;
      case DECIMAL9:
        reader.copyAsValue(writer.decimal9(outputName));
        break;
      case DECIMAL18:
        reader.copyAsValue(writer.decimal18(outputName));
        break;
      case UINT1:
        reader.copyAsValue(writer.uInt1(outputName));
        break;
      case UINT2:
        reader.copyAsValue(writer.uInt2(outputName));
        break;
      case UINT4:
        reader.copyAsValue(writer.uInt4(outputName));
        break;
      case UINT8:
        reader.copyAsValue(writer.uInt8(outputName));
        break;
      case BIGINT:
        reader.copyAsValue(writer.bigInt(outputName));
        break;
      case BIT:
        reader.copyAsValue(writer.bit(outputName));
        break;
      case VARBINARY:
      case FIXEDBINARY:
        reader.copyAsValue(writer.varBinary(outputName));
        break;
      default:
        throw new UnsupportedOperationException(
          "Cannot convert field: " + outputName + ": " + vector);
    }
  }
}
