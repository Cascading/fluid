# Fluid - A Fluent Java API for Cascading

## Overview

Fluid is an API library exposing the [Cascading](http://cascading.org/) library as a
[Fluent API](http://en.wikipedia.org/wiki/Fluent_interface).

```java
    // Fluid has factories for all Operations (Functions, Filters, Aggregators, and Buffers)
    Function splitter = Fluid.function()
      .RegexSplitter()
      .fieldDeclaration( fields( "num", "char" ) )
      .patternString( " " )
      .end();

    // Use Fluid to start an assembly builder for chaining Pipes into complex assemblies
    AssemblyBuilder.Start assembly = Fluid.assembly();

    Pipe pipeLower = assembly
      .startBranch( "lower" )
      .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
      .completeBranch();

    Pipe pipeUpper = assembly
      .startBranch( "upper" )
      .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
      .completeBranch();

    Pipe coGroup = assembly
      .startCoGroup()
      .lhs( pipeLower ).lhsGroupFields( fields( "num" ) )
      .rhs( pipeUpper ).rhsGroupFields( fields( "num" ) )
      .declaredFields( fields( "num1", "char1", "num2", "char2" ) )
      .joiner( new OuterJoin() )
      .createCoGroup();

    assembly
      .continueBranch( "result", coGroup )
      .retain( fields( "num1", "char1" ) )
      .rename( Fields.ALL, fields( "num", "char" ) )
      .completeBranch();

    // Capture all the unattached tail pipes
    Pipe[] tails = assembly.completeAssembly();

    FlowDef flowDef = flowDef()
      .addSource( "lower", sourceLower )
      .addSource( "upper", sourceUpper )
      .addSink( "result", sink )
      .addTails( tails );
```

The Fluid API is generated directly from Cascading compiled libraries.

Code generation dramatically reduces the amount of maintenance required, and will ultimately allow any third-party
classes to also have Fluent APIs generated through the build process.

To use Fluid, there is no installation. All Fluid libraries are available through the [Conjars.org](http://conjars.org)
Maven repository.

## Learning the API

Current release Java docs can be found here:

  * http://docs.cascading.org/fluid/1.1/javadoc/fluid-api/

Note that there is only one entry point, the `Fluid` class.

Your IDE should offer auto complete suggestions after calling the initial factory methods. And for the most part,
only methods will be suggested that would logically be next in the chain. Fluid strives to not add any new concepts
to its API and to only mirror Cascading concepts, so familiarity with Cascading should be sufficient, but is at
least required.

Also [Cascading for the Impatient](http://docs.cascading.org/impatient/) has been ported to Fluid.

This is [part 6](http://docs.cascading.org/impatient/impatient6.html) using the Fluid API:

  * https://github.com/Cascading/Impatient/blob/fluid/part6/src/main/java/impatient/Main.java

The source to the complete ported Impatient series:

  * https://github.com/Cascading/Impatient/tree/fluid

## Using with Maven/Ivy/Gradle

It is strongly recommended developers pull Fluid from our Maven compatible jar repository
[Conjars.org](http://conjars.org).

You can find the latest public releases here:

*  http://conjars.org/cascading/fluid-api
*  http://conjars.org/cascading/fluid-cascading25
*  http://conjars.org/cascading/fluid-cascading26
*  http://conjars.org/cascading/fluid-cascading27
*  http://conjars.org/cascading/fluid-cascading30
*  http://conjars.org/cascading/fluid-cascading31
*  http://conjars.org/cascading/fluid-cascading32

Three dependencies must be added to the project settings.

First, the `fluid-api` which contains the root `Fluid` class.

Second, the dependency that corresponds to the version of Cascading you wish to use. This artifact already has a
dependency on a particular version of Cascading. You can override this in your project to get a later maintenance
release. But do not mix/match major/minor releases this way.

Source and Javadoc artifacts (using the appropriate classifier) are also available through Conjars.

All Fluid artifacts are built with JDK 1.7 (though they are likely JDK 1.6 source compatible).

## Design Notes

Please comment on the mail list for suggestions, or prototype them in tests and pull requests.

The primary goal of Fluid is:

    To allow not only hard things to be possible, but to keep simple things simple

Thus our design goals are:

 * Easy entry and exit from the fluent API with existing Cascading objects
 * Mirror the existing Cascading library as much as possible to minimize the number of concepts to be learned
 * Allow custom Cascading classes to have fluent APIs generated within any Cascading based project
 * Provide tools to allow for (more productive) higher order
 [internal DSLs](http://martinfowler.com/bliki/InternalDslStyle.html) to be created and distributed

The last point is important. Fluid is not meant to compete with [Scalding](http://www.cascading.org/projects/scalding/)
and [Cascalog](http://www.cascading.org/projects/cascalog/), but should allow users
who wish to stay in Java-land the opportunity to use a self guiding API and/or craft special purpose APIs for
particular needs.

Code generation is possible because Cascading has a clear uniform convention to how Pipe and Operation classes are
defined, and through the Java `ConstructorProperties` Annotation set on every core object constructors. This
includes `SubAssemblies` which are treated as regular Pipes, and thus are chained.

In the near term, we hope to have a gradle plugin for generating APIs from existing code to be released with those
libraries.

In time we hope Fluid will provide a base for incorporating Java 8 lambdas.

## Reporting Issues

The best way to report an issue is to mail the
[mailing list](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) with a clear description.

If verified as a potential issues, add a new test along with the expected result set and submit a pull request
on GitHub.

Failing that, feel free to open an [issue](https://github.com/Cascading/fluid/issues) on the
[Cascading/Fluid](https://github.com/Cascading/fluid) project site.

## Developing

Fluid currently requires at least Gradle 3.4 to build.

Running:

    > gradle idea

from the root of the project will create all IntelliJ project and module files, and retrieve all dependencies.

Fluid is based on the open-source project [UnquietCode/Flapi](https://github.com/UnquietCode/Flapi).

## WIP Releases

Work in progress (WIP) code can be found on [GitHub](https://github.com/Cascading/fluid), under a branch named
`wip-x.y`. Final releases will be branches named `x.y`, for a given release version.

WIP builds, if available, can generally be found on conjars.org, see above, or:

  * http://www.cascading.org/wip/

Current wip Java docs can be found here:

  * http://docs.concurrentinc.com/fluid/1.1/javadoc/fluid-api/

## Notes

  * There currently are no builders for Tap or Scheme types
  * We plan to provide a gradle plugin for API generation in a future release