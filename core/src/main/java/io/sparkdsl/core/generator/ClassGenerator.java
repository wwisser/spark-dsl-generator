package io.sparkdsl.core.generator;

import io.sparkdsl.core.schema.Schema;

public interface ClassGenerator {
  GeneratedClass generateClass(Schema schema);
}
