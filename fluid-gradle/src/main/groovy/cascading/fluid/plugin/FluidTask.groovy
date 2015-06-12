package cascading.fluid.plugin

import cascading.fluid.generator.CascadingRunner
import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import org.reflections.Reflections

/**
 */
public class FluidTask extends DefaultTask
{

  /**
   * The directory from which the existing sources
   * will be read.
   */
  @Input
  @InputDirectory
  File inputSources;

  /**
   * The directory from which the existing classes
   * will be read.
   */
  @Input
  @InputDirectory
  File inputClasses

  /**
   * The directory to which the generated sources
   * will be written.
   */
  @Input
  @Optional
  @OutputDirectory
  File outputSources;

  /**
   * The list of package prefixes to scan for
   * Cascading classes.
   */
  @Input
  String[] packages = [];

  /**
   * The package name to use as the base for
   * generated classes.
   */
  @Input
  String basePackage;

  /**
   * Include cascading core classes.
   */
  boolean includeCascading = false;

  public FluidTask()
  {
    // fluid tasks depend on compilation
    this.dependsOn( project.tasks.getByName( 'compileJava' ) )

    // the full set of of classes depends on this task
    project.tasks.getByName( 'classes' ).dependsOn( this )

    inputClasses = project.sourceSets.main.output.classesDir
    inputSources = project.sourceSets.main.java.srcDirs[ 0 ];
    outputSources = new File( project.buildDir.getAbsolutePath(), "generated-sources" )
  }

  @TaskAction
  void handle()
  {

    // base package is required
    if( isEmpty( basePackage ) )
      throw new RuntimeException( "base package cannot be empty" )

    // create a custom classloader
    def classLoader = getClassLoader()
    Thread.currentThread().setContextClassLoader( classLoader )

    // build up the reflection helper
    List<Object> args = new ArrayList<>();
    args.add( classLoader );
    args.add( 'cascading' )
    args.addAll( Arrays.asList( packages ) );

    def reflectionHelper = new Reflections( args.toArray( new Object[args.size()] ) );

    def runner = includeCascading ?
      new CascadingRunner( inputSources, outputSources )
      : new CustomRunner( inputSources, outputSources, basePackage )

    try
    {
      runner.execute( reflectionHelper )
    }
    catch( e )
    {
      throw new RuntimeException( "error while generating fluid builder", e )
    }
  }

  private URLClassLoader getClassLoader() throws Exception
  {
    List<URL> urls = new ArrayList<>();

    // dependencies
    for( def path : project.configurations[ 'compile' ] )
      urls.add( path.toURI().toURL() )

    // project sources
    final URL sourceDir;

    if( inputClasses == null )
      sourceDir = project.sourceSets.main.output.classesDir.toURI().toURL()
    else
      sourceDir = inputClasses.toURI().toURL()

    urls.add( sourceDir )

    return new URLClassLoader( urls.toArray( new URL[urls.size()] ), getClass().getClassLoader() );
  }

  private static boolean isEmpty( String s )
  {
    return s == null || s.trim().isEmpty()
  }
}