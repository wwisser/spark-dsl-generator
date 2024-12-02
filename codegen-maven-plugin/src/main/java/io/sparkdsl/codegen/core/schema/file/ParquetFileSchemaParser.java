package io.sparkdsl.codegen.core.schema.file;

import io.sparkdsl.codegen.core.schema.Schema;
import io.sparkdsl.codegen.core.schema.SchemaParser;
import io.sparkdsl.codegen.core.schema.SchemaSourceType;
import java.io.IOException;
import java.util.Collections;
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

public class ParquetFileSchemaParser implements SchemaParser<String> {

  @Override
  public Set<Schema> parseSchema(String filePath) {
    try {
      Configuration configuration = new Configuration();
      configuration.set("fs.defaultFS", "file:///");

      InputFile inputFile = HadoopInputFile.fromPath(new Path(filePath), configuration);

      try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
        ParquetMetadata parquetMetadata = reader.getFooter();
        MessageType schema = parquetMetadata.getFileMetaData().getSchema();

        Set<String> fields =
            schema.getColumns().stream()
                .map(ColumnDescriptor::getPath)
                .map(p -> p[0])
                .collect(Collectors.toSet());

        return Collections.singleton(new Schema(schema.getName(), fields));
      }
    } catch (IOException e) {
      throw new RuntimeException("failed to parse parquet schema from file", e);
    }
  }

  @Override
  public SchemaSourceType getSchemaSourceType() {
    return SchemaSourceType.PARQUET_FILE;
  }
}
