package io.sparkdsl.codegen.core.classwriter;

import io.sparkdsl.codegen.core.generator.GeneratedClass;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Helo
 */
public class ClassWriter {

  public void writeJavaFileDef(GeneratedClass clazz, String absolutePath, String destination) {
    try {
      Path outputPath = Paths.get(absolutePath, destination, clazz.getFileName());

      Files.createDirectories(outputPath.getParent());
      Files.write(outputPath, clazz.getJavaDef().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException("failed writing generated class def as file", e);
    }
  }
}
