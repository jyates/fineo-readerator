package io.fineo.read.drill.exec.store.rel;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.expr.holders.UInt1Holder;
import org.apache.drill.exec.expr.holders.UInt2Holder;
import org.apache.drill.exec.expr.holders.UInt4Holder;
import org.apache.drill.exec.expr.holders.UInt8Holder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

public class VectorUtils {

  private VectorUtils() {
  }

  public static void write(String outputName, VectorWrapper wrapper, BaseWriter.MapWriter writer,
    int index) {
    write(outputName, wrapper.getValueVector(), wrapper.getField().getType(), writer,
      index, index);
  }

  public static void write(String name, VectorWrapper wrapper, BaseWriter.MapWriter writer,
    int inIndex, int outIndex) {
    write(name, wrapper.getValueVector(), wrapper.getField().getType(), writer,
      inIndex, outIndex);
  }

  public static void write(String outputName, ValueVector vector, TypeProtos.MajorType major,
    BaseWriter.MapWriter writer, int inIndex, int outIndex) {
    writer.setPosition(outIndex);
    TypeProtos.MinorType type = major.getMinorType();

    switch (major.getMode()) {
      case REPEATED:
        throw new UnsupportedOperationException("Don't support repeated fields!");
      case REQUIRED:
        writeRequired(type, outputName, BasicTypeHelper.getValue(vector, inIndex), writer);
        break;
      case OPTIONAL:
        FieldReader reader =  vector == null ? NullReader.INSTANCE : vector.getReader();
        reader.setPosition(inIndex);
        writeNullable(type, outputName, reader, writer);
    }
  }

  private static void writeRequired(TypeProtos.MinorType type, String outputName,
    ValueHolder holder, BaseWriter.MapWriter writer) {
    switch (type) {
      case FIXED16CHAR:
      case VARCHAR:
        writer.varChar(outputName).write((VarCharHolder) holder);
        break;
      case FLOAT4:
        writer.float4(outputName).write((Float4Holder) holder);
        break;
      case FLOAT8:
        writer.float8(outputName).write((Float8Holder) holder);
        break;
      case INT:
        writer.integer(outputName).write((IntHolder) holder);
        break;
      case SMALLINT:
        writer.smallInt(outputName).write((SmallIntHolder) holder);
        break;
      case TINYINT:
        writer.tinyInt(outputName).write((TinyIntHolder) holder);
        break;
      case DECIMAL9:
        writer.decimal9(outputName).write((Decimal9Holder) holder);
        break;
      case DECIMAL18:
        writer.decimal18(outputName).write((Decimal18Holder) holder);
        break;
      case UINT1:
        writer.uInt1(outputName).write((UInt1Holder) holder);
        break;
      case UINT2:
        writer.uInt2(outputName).write((UInt2Holder) holder);
        break;
      case UINT4:
        writer.uInt4(outputName).write((UInt4Holder) holder);
        break;
      case UINT8:
        writer.uInt8(outputName).write((UInt8Holder) holder);
        break;
      case BIGINT:
        writer.bigInt(outputName).write((BigIntHolder) holder);
        break;
      case BIT:
        writer.bit(outputName).write((BitHolder) holder);
        break;
      case VARBINARY:
      case FIXEDBINARY:
        writer.varBinary(outputName).write((VarBinaryHolder) holder);
        break;
      default:
        throw new UnsupportedOperationException("Cannot convert field: " + outputName);
    }
  }

  private static void writeNullable(TypeProtos.MinorType type, String outputName, FieldReader
    reader, BaseWriter.MapWriter writer) {
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
        throw new UnsupportedOperationException("Cannot convert field: " + outputName);
    }
  }
}
