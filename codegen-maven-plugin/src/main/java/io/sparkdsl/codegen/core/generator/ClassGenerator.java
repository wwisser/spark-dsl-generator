package io.sparkdsl.codegen.core.generator;

import io.sparkdsl.codegen.core.schema.Schema;

public interface ClassGenerator {
  GeneratedClass generateClass(Schema schema);
}
