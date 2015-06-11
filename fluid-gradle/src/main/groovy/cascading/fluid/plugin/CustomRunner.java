package cascading.fluid.plugin;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import cascading.fluid.generator.Runner;
import cascading.fluid.generator.builder.Generator;
import cascading.fluid.generator.builder.OperationsGenerator;
import cascading.fluid.generator.builder.SubAssembliesGenerator;
import cascading.fluid.generator.javadocs.DocsHelper;
import org.reflections.Reflections;

/**
 */
public class CustomRunner extends Runner
  {
  private final String packageName;

  public CustomRunner( File sourceDir, File outputDir, String packageName )
    {
    super( sourceDir, outputDir );
    this.packageName = Objects.requireNonNull( packageName );
    }

  public CustomRunner( String sourceDir, String outputDir, String packageName )
    {
    super( sourceDir, outputDir );
    this.packageName = Objects.requireNonNull( packageName );
    }

  @Override
  protected List<Generator> generators( DocsHelper documentationHelper )
    {
    throw new UnsupportedOperationException( "not implemented" );
    }

  @Override
  protected List<Generator> generators( DocsHelper documentationHelper, Reflections reflectionHelper )
    {
    OperationsGenerator operationsGenerator = new OperationsGenerator( documentationHelper, reflectionHelper );
    operationsGenerator.setPackageName( packageName );

    SubAssembliesGenerator subAssembliesGenerator = new SubAssembliesGenerator( documentationHelper, reflectionHelper );
    subAssembliesGenerator.setPackageName( packageName );

    return Arrays.<Generator>asList(
        operationsGenerator,
        subAssembliesGenerator
    );

    }
  }