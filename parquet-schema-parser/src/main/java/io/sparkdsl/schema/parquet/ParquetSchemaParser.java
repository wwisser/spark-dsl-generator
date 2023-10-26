package io.sparkdsl.schema.parquet;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

public class ParquetSchemaParser {

  public static void main(String[] args) throws IOException {
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "file:///");

    InputFile inputFile = HadoopInputFile.fromPath(new Path("sample.parquet"), new Configuration());

    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata parquetMetadata = reader.getFooter();
      MessageType schema = parquetMetadata.getFileMetaData().getSchema();

      VelocityEngine velocityEngine = new VelocityEngine();
      velocityEngine.addProperty("resource.loader", "classpath");
      velocityEngine.addProperty("classpath.resource.loader.class",
          ClasspathResourceLoader.class.getName());
      velocityEngine.init();

      Set<String> fields = schema.getColumns().stream().map(ColumnDescriptor::getPath)
          .map(p -> p[0]).collect(Collectors.toSet());

      VelocityContext context = new VelocityContext();
      context.put("className", schema.getName());
      context.put("fields", fields);

      Template template = velocityEngine.getTemplate("class-template.vm");
      StringWriter writer = new StringWriter();
      template.merge(context, writer);

      System.out.println(writer);
    }
  }

}
