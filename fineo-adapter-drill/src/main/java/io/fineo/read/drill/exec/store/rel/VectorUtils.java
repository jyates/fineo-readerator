package io.fineo.read.drill.exec.store.rel;

import org.apache.drill.common.types.TypeProtos;
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
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal18Writer;
import org.apache.drill.exec.vector.complex.writer.Decimal9Writer;
import org.apache.drill.exec.vector.complex.writer.Float4Writer;
import org.apache.drill.exec.vector.complex.writer.Float8Writer;
import org.apache.drill.exec.vector.complex.writer.IntWriter;
import org.apache.drill.exec.vector.complex.writer.SmallIntWriter;
import org.apache.drill.exec.vector.complex.writer.TinyIntWriter;
import org.apache.drill.exec.vector.complex.writer.UInt1Writer;
import org.apache.drill.exec.vector.complex.writer.UInt2Writer;
import org.apache.drill.exec.vector.complex.writer.UInt4Writer;
import org.apache.drill.exec.vector.complex.writer.UInt8Writer;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;

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
    FieldReader reader = vector.getReader();
    reader.setPosition(inIndex);
    writer.setPosition(outIndex);
    TypeProtos.MinorType type = major.getMinorType();

    switch (major.getMode()) {
      case REPEATED:
        throw new UnsupportedOperationException("Don't support repeated fields!");
      case REQUIRED:
        writeRequired(type, outputName, reader, writer);
        break;
      case OPTIONAL:
        writeNullable(type, outputName, reader, writer);
    }
  }

  private static void writeRequired(TypeProtos.MinorType type, String outputName,
    FieldReader reader, BaseWriter.MapWriter writer) {
    ValueHolder holder;
    switch (type) {
      case FIXED16CHAR:
      case VARCHAR:
        VarCharWriter vcw = writer.varChar(outputName);
        holder = new VarCharHolder();
        reader.read((VarCharHolder) holder);
        vcw.write((VarCharHolder) holder);
        break;
      case FLOAT4:
        Float4Writer f4w = writer.float4(outputName);
        holder = new Float4Holder();
        reader.read((Float4Holder) holder);
        f4w.write((Float4Holder) holder);
        break;
      case FLOAT8:
        Float8Writer f8w = writer.float8(outputName);
        holder = new Float8Holder();
        reader.read((Float8Holder) holder);
        f8w.write((Float8Holder) holder);
        break;
      case INT:
        IntWriter iw = writer.integer(outputName);
        holder = new IntHolder();
        reader.read((IntHolder) holder);
        iw.write((IntHolder) holder);
        break;
      case SMALLINT:
        SmallIntWriter siw = writer.smallInt(outputName);
        holder = new SmallIntHolder();
        reader.read((SmallIntHolder) holder);
        siw.write((SmallIntHolder) holder);
        break;
      case TINYINT:
        TinyIntWriter tiw = writer.tinyInt(outputName);
        holder = new TinyIntHolder();
        reader.read((TinyIntHolder) holder);
        tiw.write((TinyIntHolder) holder);
        break;
      case DECIMAL9:
        Decimal9Writer d9w = writer.decimal9(outputName);
        holder = new Decimal9Holder();
        reader.read((Decimal9Holder) holder);
        d9w.write((Decimal9Holder) holder);
        break;
      case DECIMAL18:
        Decimal18Writer d18w = writer.decimal18(outputName);
        holder = new Decimal18Holder();
        reader.read((Decimal18Holder) holder);
        d18w.write((Decimal18Holder) holder);
        break;
      case UINT1:
        UInt1Writer ui1w = writer.uInt1(outputName);
        holder = new UInt1Holder();
        reader.read((UInt1Holder) holder);
        ui1w.write((UInt1Holder) holder);
        break;
      case UINT2:
        UInt2Writer ui2w = writer.uInt2(outputName);
        holder = new UInt2Holder();
        reader.read((UInt2Holder) holder);
        ui2w.write((UInt2Holder) holder);
        break;
      case UINT4:
        UInt4Writer ui4w = writer.uInt4(outputName);
        holder = new UInt4Holder();
        reader.read((UInt4Holder) holder);
        ui4w.write((UInt4Holder) holder);
        break;
      case UINT8:
        UInt8Writer ui8w = writer.uInt8(outputName);
        holder = new UInt8Holder();
        reader.read((UInt8Holder) holder);
        ui8w.write((UInt8Holder) holder);
        break;
      case BIGINT:
        BigIntWriter biw = writer.bigInt(outputName);
        holder = new BigIntHolder();
        reader.read((BigIntHolder) holder);
        biw.write((BigIntHolder) holder);
        break;
      case BIT:
        BitWriter bw = writer.bit(outputName);
        holder = new BitHolder();
        reader.read((BitHolder) holder);
        bw.write((BitHolder) holder);
        break;
      case VARBINARY:
      case FIXEDBINARY:
        VarBinaryWriter binw = writer.varBinary(outputName);
        holder = new VarBinaryHolder();
        reader.read((VarBinaryHolder) holder);
        binw.write((VarBinaryHolder) holder);
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
