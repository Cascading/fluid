package cascading.fluid.plugin

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction
import org.reflections.Reflections

/**
 * @author Ben Fagin
 * @version 2015-03-23
 */
public class FluidTask extends DefaultTask {

    public FluidTask() {

        // fluid tasks depend on compilation
        this.dependsOn( project.tasks.getByName( 'compileJava' ))

        // the full set of of classes depends on this task
        project.tasks.getByName( 'classes' ).dependsOn( this )
    }

    /**
     * The directory from which the existing sources
     * will be read.
     */
    String inputSources;

    /**
     * The directory from which the existing classes
     * will be read.
     */
    String inputClasses

    /**
     * The directory to which the generated sources
     * will be written.
     */
    String outputSources;

    /**
     * The list of package prefixes to scan for
     * Cascading classes.
     */
    String[] packages = [];

    /**
     * The package name to use as the base for
     * generated classes.
     */
    String basePackage;

    /**
     * Include cascading core classes.
     */
    boolean includeCascading = false;


    @TaskAction
    void handle()
        {

        // base package is required
        if (isEmpty( basePackage )) {
            throw new RuntimeException("base package cannot be empty")
        }

        // default for sources directory
        if (isEmpty( inputSources )) {
            inputSources = "${project.projectDir}/src/main/java"
        }

        // default for classes directory
        if (isEmpty( outputSources )) {
            outputSources = project.buildDir.getAbsolutePath()+File.separator+"generated-sources"
        }

        // create a custom classloader
        def classLoader = getClassLoader()
        Thread.currentThread().setContextClassLoader( classLoader )

        // build up the reflection helper
        List<Object> args = new ArrayList<>();
        args.add( classLoader );
        args.add( 'cascading' )
        args.addAll( Arrays.asList( packages ));

        def reflectionHelper = new Reflections( args.toArray( new Object[args.size()] ));

        def runner = new CustomRunner( inputSources, outputSources, basePackage )
        runner.setIncludeCascading( includeCascading )

        try {
            runner.execute( reflectionHelper )
        } catch (e) {
            throw new RuntimeException( "error while generating fluid builder", e )
        }

        }

    private URLClassLoader getClassLoader() throws Exception
        {
        List<URL> urls = new ArrayList<>();

        // dependencies
        for (def path : project.configurations['compile'])
            {
            urls.add( path.toURI().toURL() )
            }

        // project sources
        final URL sourceDir;

        if (isEmpty( inputClasses )) {
            sourceDir = project.sourceSets.main.output.classesDir.toURI().toURL()
        } else {
            sourceDir = new File( inputClasses ).toURI().toURL()
        }

        urls.add( sourceDir )

        return new URLClassLoader(urls.toArray(new URL[urls.size()]), getClass().getClassLoader() );
        }

	private static boolean isEmpty(String s)
        {
        return s == null || s.trim().isEmpty()
        }
}