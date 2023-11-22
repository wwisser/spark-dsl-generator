package io.sparkdsl.codegen.maven;

import static org.apache.maven.plugins.annotations.LifecyclePhase.GENERATE_SOURCES;
import static org.apache.maven.plugins.annotations.ResolutionScope.TEST;

import io.sparkdsl.core.classwriter.ClassWriter;
import io.sparkdsl.core.generator.GeneratedClass;
import io.sparkdsl.core.generator.VelocityClassGenerator;
import io.sparkdsl.core.schema.ParquetFileSchemaParser;
import io.sparkdsl.core.schema.Schema;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

@Mojo(
    name = "generate",
    defaultPhase = GENERATE_SOURCES,
    requiresDependencyResolution = TEST,
    threadSafe = true)
public class CodegenMojo extends AbstractMojo {

  private static final String TARGET_DIR = "target/generated-sources/sparkdsl";

  @Parameter(property = "project", required = true, readonly = true)
  private MavenProject project;

  @Parameter(property = "configFile")
  private String configFile;

  @Override
  public void execute() {
    Schema schema = new ParquetFileSchemaParser().parseSchema(configFile);
    GeneratedClass classDef = new VelocityClassGenerator().generateClass(schema);

    new ClassWriter()
        .writeJavaFileDef(classDef, project.getBasedir().getAbsolutePath(), TARGET_DIR);
  }
}
