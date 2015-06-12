# Fluid plugin for Gradle

This plugin provides functionality to scan for Cascading classes and generate
fluent builders for them.

To run, add the plugin to your Gradle project like so:

```groovy
buildscript {
  repositories {
    maven {url 'http://conjars.org/repo/'}
    maven {url = 'http://www.unquietcode.com/maven/releases'}
  }
	dependencies {
		classpath('cascading:fluid-gradle:1.1.0')
	}
}

task fluidGenerator(type: cascading.fluid.plugin.FluidTask) {
  packages = ['packages.to.consider']
  basePackage = 'base.package.for.output'
}
```

The task will by default be run after the java class generation task and before the test classes are processed.

Dependencies referenced in classes should be made available on the compile classpath so that the
javadoc scanner can be most effective.

For cascading builds, the special flag 'includeCascading' should be provided.

## Parameters
Gradle task parameters for the `fluid` task.

### Required
	* `packages` - The list of package prefixes to scan for Cascading classes.
	* `basePackage` - The package name to use as the base for generated classes.

### Optional
	* `inputClasses` - The directory from which the existing classes will be read. (default `build/classes`)
	* `inputSources` - The directory from which the existing sources will be read. (default project sources)
	* `outputSources` - The directory to which the generated sources will be written. (default `build/generated-sources`)
	* `includeCascading` - Include cascading core classes. (default `false`)

