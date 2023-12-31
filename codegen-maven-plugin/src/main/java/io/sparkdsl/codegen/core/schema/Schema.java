package io.sparkdsl.codegen.core.schema;

import java.util.Set;
import lombok.Value;

@Value
public class Schema {
  String name;
  Set<String> fields;
}
