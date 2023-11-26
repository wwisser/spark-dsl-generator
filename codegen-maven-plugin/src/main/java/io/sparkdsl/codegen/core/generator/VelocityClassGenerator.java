package io.sparkdsl.codegen.core.generator;

import io.sparkdsl.codegen.core.schema.Schema;
import java.io.StringWriter;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

public class VelocityClassGenerator implements ClassGenerator {

  private static final String TEMPLATE_FILE = "class-template.vm";
  private static final String FILE_EXTENSION = ".java";

  private final VelocityEngine engine;

  public VelocityClassGenerator() {
    engine = new VelocityEngine();
    engine.addProperty("resource.loader", "classpath");
    engine.addProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
    engine.init();
  }

  @Override
  public GeneratedClass generateClass(Schema schema) {
    VelocityContext context = new VelocityContext();

    String schemaName = schema.getName();
    context.put("className", schemaName);
    context.put("fields", schema.getFields());

    Template template = engine.getTemplate(TEMPLATE_FILE);
    StringWriter writer = new StringWriter();
    template.merge(context, writer);

    return new GeneratedClass(schemaName + FILE_EXTENSION, writer.toString());
  }
}
