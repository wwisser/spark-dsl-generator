package com.sparkdslgenerator.schema.parquet;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetSchemaParser {

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: ParquetSchemaParser <parquet-file-path>");
      System.exit(1);
    }

    InputFile inputFile = HadoopInputFile.fromPath(new Path(args[0]), new Configuration());

    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata parquetMetadata = reader.getFooter();
      MessageType schema = parquetMetadata.getFileMetaData().getSchema();

      System.out.println("Parquet Schema:");
      System.out.println(schema);
    }
  }

}
