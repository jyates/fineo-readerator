package io.fineo.read.drill.udf.conv;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;

import javax.inject.Inject;

@FunctionTemplate(name = "fineo_base64_decode",
                  scope = FunctionTemplate.FunctionScope.SIMPLE,
                  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class Base64Decoder implements DrillSimpleFunc {

  @Param NullableVarBinaryHolder in1;
  @Output VarBinaryHolder out;

  // holder for the output of the translation
  @Inject
  DrillBuf buffer;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
    int length = in1.end - in1.start;
    io.netty.buffer.ByteBuf buff =
      io.netty.handler.codec.base64.Base64.decode(in1.buffer, in1.start, length);
    int outputSize = buff.readableBytes();
    out.buffer = buffer.reallocIfNeeded(outputSize);
    out.start = 0;
    out.end = outputSize;
    buffer.setBytes(0, buff);
  }
}
