# Fluid - A Fluent API for [Cascading](http://cascading.org/)

## Overview

Fluid is an API library exposing the Cascading library as a 
[Fluent API](http://en.wikipedia.org/wiki/Fluent_interface).

````java
    // Factories for all Operations (Functions, Filters, Aggregators, and Buffers)
    Function splitter = Fluid.function()
      .RegexSplitter()
      .fieldDeclaration( fields( "num", "char" ) )
      .patternString( " " )
      .end();

    // An assembly builder chaining Pipes into complex assemblies
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

    Pipe[] tails = assembly.completeAssembly();

    FlowDef flowDef = flowDef()
      .addSource( "lower", sourceLower )
      .addSource( "upper", sourceUpper )
      .addSink( "result", sink )
      .addTails( tails );
```` 

The Fluid API is generated directly from Cascading compiled libraries. 

Code generation dramatically reduces the amount of maintenance required, and allows any third-party classes to also 
have Fluent APIs generated through the build process.
 
Fluid is under active development on the wip-1.0 branch. This means it is currently a work in progress and subject
to change prior to the 1.0 release.

To use Fluid, there is no installation. All Fluid libraries are available through the [Conjars.org](http://conjars.org) 
Maven repository.

## Learning the API

Current wip Java docs can be found here:

  * http://docs.concurrentinc.com/fluid/1.0/javadoc/fluid-api/

Note that there is only one entry point, the `Fluid` class. 

Your IDE should offer auto complete suggestions after calling the initial factory methods. And for the most part,
only methods will be suggested that would logically be next in the chain.

## Using with Maven/Ivy/Gradle

It is strongly recommended developers pull Fluid from our Maven compatible jar repository
[Conjars.org](http://conjars.org).

You can find the latest public and WIP (work in progress) releases here:

*  http://conjars.org/cascading/fluid-api
*  http://conjars.org/cascading/fluid-cascading25
*  http://conjars.org/cascading/fluid-cascading26
*  http://conjars.org/cascading/fluid-cascading30

Two dependencies must be added to the project settings. 

First, the `fluid-api` which contains the root `Fluid` class.

Second, the dependency that corresponds to the version of Cascading you wish to use. This artifact already has a
dependency on a particular version of Cascading. You can override this in your project to get a later maintenance
release. But do not mix/match major/minor releases this way.

Source and Javadoc artifacts (using the appropriate classifier) are also available through Conjars.

All Fluid artifacts are built with JDK 1.7 (though they are likely JDK 1.6 source compatible).

## Design Notes

The API itself and build interfaces are still under development. Please comment on the mail list for suggestions, or 
prototype them in tests and pull requests.

The primary goal of Fluid is: 

    To allow not only hard things to be possible, but to keep simple things simple    
    
Thus our design goals are:
    
 * Easy entry and exit from the fluent API with existing Cascading objects
 * Mirror the existing Cascading library as much as possible to minimize the number of concepts to be learned
 * Allow custom Cascading classes to have fluent APIs generated within any Cascading based project
 * Provide tools to allow for (more productive) higher order 
 [internal DSLs](http://martinfowler.com/bliki/InternalDslStyle.html) to be created and distributed

The last point is important. Fluid is not meant to compete with Scalding and Cascalog, but should allow users 
who wish to stay in Java-land the opportunity to use a self guiding API and/or craft special purpose APIs for particular
needs.

Code generation is possible because Cascading has a clear uniform convention to how Pipe and Operation classes are 
defined, and through the Java `ConstructorProperties` Annotation set on every core object constructors. This 
includes `SubAssemblies` which are treated as regular Pipes, and thus are chained.

In the near term, we hope to have a gradle plugin for generating APIs from existing code to be released with those
libraries.
              
In time we hope Fluid will provide a base for incorporating Java 8 lambdas.              

## Reporting Issues

The best way to report an issue is to add a new test along with the expected result set
and submit a pull request on GitHub.

Failing that, feel free to open an [issue](https://github.com/Cascading/fluid/issues) on the 
[Cascading/Fluid](https://github.com/Cascading/fluid)
project site or mail the [mailing list](https://groups.google.com/forum/?fromgroups#!forum/cascading-user).

## Developing

Running:

    > gradle idea

from the root of the project will create all IntelliJ project and module files, and retrieve all dependencies.

Fluid is based on the open-source project [UnquietCode/Flapi](https://github.com/UnquietCode/Flapi).
