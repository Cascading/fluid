package cascading.fluid.generator;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import cascading.fluid.generator.builder.AssemblyGenerator;
import cascading.fluid.generator.builder.Generator;
import cascading.fluid.generator.builder.OperationsGenerator;
import cascading.fluid.generator.builder.SubAssembliesGenerator;
import cascading.fluid.generator.javadocs.DocsHelper;
import org.reflections.Reflections;

/**
 */
public class CascadingRunner extends Runner
  {
  public CascadingRunner( File sourceDir, File outputDir )
    {
    super( sourceDir, outputDir );
    }

  public CascadingRunner( String sourceDir, String outputDir )
    {
    super( sourceDir, outputDir );
    }

  @Override
  protected List<Generator> generators( DocsHelper documentationHelper )
    {
    return Arrays.asList(
      new AssemblyGenerator( documentationHelper ).includeCascading(),
      new OperationsGenerator( documentationHelper ).includeCascading(),
      new SubAssembliesGenerator( documentationHelper ).includeCascading()
    );
    }

  @Override
  protected List<Generator> generators( DocsHelper documentationHelper, Reflections reflectionHelper )
    {
    return Arrays.asList(
      new AssemblyGenerator( documentationHelper, reflectionHelper ).includeCascading(),
      new OperationsGenerator( documentationHelper, reflectionHelper ).includeCascading(),
      new SubAssembliesGenerator( documentationHelper, reflectionHelper ).includeCascading()
    );
    }
  }
