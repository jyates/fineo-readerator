package io.fineo.read.drill.exec.store.rel;

import io.fineo.internal.customer.Metric;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 *
 */
public class MetricUtils {

  private MetricUtils() {
  }

  public static String getMetricString(Metric metric) throws IOException {
    DatumWriter<Metric> datumWriter = new SpecificDatumWriter<>(Metric.getClassSchema());
    DataFileWriter<Metric> dataFileWriter = new DataFileWriter<>(datumWriter);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos, true, "utf-8");
    dataFileWriter.create(Metric.getClassSchema(), ps);
    dataFileWriter.append(metric);
    dataFileWriter.close();
    return baos.toString();
  }

  public static Metric parseMetric(String metricJson) throws IOException {
    DatumReader<Metric> reader = new SpecificDatumReader<>(Metric.class);
    ByteArrayInputStream bis = new ByteArrayInputStream(metricJson.getBytes());
    DataInputStream din = new DataInputStream(bis);
    Decoder decoder = DecoderFactory.get().jsonDecoder(Metric.getClassSchema(), din);
    return reader.read(null, decoder);
  }
}
