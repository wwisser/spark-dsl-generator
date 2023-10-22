package io.sparkdsl.schema.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class ParquetFileWriter {

  private static final String HADOOP_HOME = "C:\\Users\\Wendelin\\hadoop";
  private static final String DEFAULT_FS = "file:///";
  private static final String OUTPUT_FILE = "sample.parquet";

  public static void main(String[] args) throws IOException {
    System.setProperty("hadoop.home.dir", HADOOP_HOME);

    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", DEFAULT_FS);

    Schema schema = SchemaBuilder.record("User")
        .fields()
        .requiredString("first_name")
        .requiredString("last_name")
        .requiredInt("age")
        .endRecord();

    Path path = new Path(OUTPUT_FILE);
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withConf(configuration)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {

      GenericRecord john = new GenericData.Record(schema);
      john.put("first_name", "John");
      john.put("last_name", "Doe");
      john.put("age", 30);
      writer.write(john);

      GenericRecord bob = new GenericData.Record(schema);
      bob.put("first_name", "Bob");
      bob.put("last_name", "Smith");
      bob.put("age", 45);
      writer.write(bob);

      System.out.println("Parquet file created successfully!");
    }
  }
}
