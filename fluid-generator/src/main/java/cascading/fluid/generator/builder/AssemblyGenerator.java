/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.fluid.generator.builder;

import cascading.fluid.generator.javadocs.DocsHelper;
import cascading.fluid.generator.util.Reflection;
import cascading.pipe.Checkpoint;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import org.reflections.Reflections;
import unquietcode.tools.flapi.ClassReference;
import unquietcode.tools.flapi.builder.Block.BlockBuilder;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder;

public class AssemblyGenerator extends Generator
  {
  public AssemblyGenerator( DocsHelper documentationHelper )
    {
    super( documentationHelper );
    }

  public AssemblyGenerator( DocsHelper documentationHelper, Reflections reflections )
    {
    super( documentationHelper, reflections );
    }

  @Override
  protected DescriptorBuilder.Start<?> generateInternal( String targetPath )
    {
    DescriptorBuilder.Start<?> builder = getBuilder();

    addBranchBlock( builder );

    builder.addMethod( "completeAssembly()" ).last( Pipe[].class ); // tails

    builder
      .setPackage( "cascading.fluid.internal.assembly" )
      .setDescriptorName( "Assembly" )
      .setStartingMethodName( "startAssembly" );

    return builder;
    }

  private void addBranchBlock( DescriptorBuilder.Start<?> builder )
    {
    BlockBuilder.Start<?> branch = builder
      .startBlock( "Branch", "startBranch(String name)" )
      .withDocumentation( "Begin a new branch with the given name." )
      .any();

    branch = branch
      .addMethod( "pipe(String name)" ).any();

    branch = branch
      .startBlock( "Each", "each(cascading.tuple.Fields argumentSelector)" )
      .withDocumentation()
      .addContent( "Append a new Each operator to the current branch with the given argumentSelector.\n" )
      .addContent( "@see " + Each.class.getName() )
      .finish()
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( PIPE_FACTORY ) )
      .withParameter( "creates", Each.class )
      .withParameter( "method", "each(cascading.tuple.Fields argumentSelector)" )
      .finish()
      .any( EACH )

      .startBlock( "function(cascading.operation.Function function)" ).last()
      .addMethod( "outgoing(cascading.tuple.Fields outgoingSelector)" ).last()
      .endBlock() // function

      .addMethod( "filter(cascading.operation.Filter filter)" ).last() // debug

      .startBlock( "debugLevel(cascading.operation.DebugLevel debugLevel)" ).last()
      .addMethod( "debug(cascading.operation.Debug debug)" ).last()
      .endBlock() // debug

      .startBlock( "assertionLevel(cascading.operation.AssertionLevel assertionLevel)" ).last()
      .addMethod( "assertion(cascading.operation.ValueAssertion assertion)" ).last()
      .endBlock() // debug

      .endBlock(); // each

    branch = branch
      .startBlock( "GroupBy", "groupBy(cascading.tuple.Fields groupFields)" )
      .withDocumentation()
      .addContent( "Append a new GroupBy pipe to the current branch with the given groupFields.\n" )
      .addContent( "@see " + GroupBy.class.getName() )
      .finish()
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( PIPE_FACTORY ) )
      .withParameter( "creates", GroupBy.class )
      .withParameter( "createOnNext", true )
      .withParameter( "method", "groupBy(cascading.tuple.Fields groupFields)" )
      .finish()
      .any( GROUP )

      .startBlock( "Every", "every(cascading.tuple.Fields argumentSelector)" )//.after( GROUP )
      .withDocumentation()
      .addContent( "Append a new Every operator to the current branch with the given argumentSelector.\n" )
      .addContent( "@see " + Every.class.getName() )
      .finish()
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( PIPE_FACTORY ) )
      .withParameter( "creates", Every.class )
      .withParameter( "method", "every(cascading.tuple.Fields argumentSelector)" )
      .finish()
      .any( EVERY )

      .startBlock( "aggregator(cascading.operation.Aggregator aggregator)" ).last()
      .addMethod( "outgoing(cascading.tuple.Fields outgoingSelector)" ).last()
      .endBlock() // aggregator

      .startBlock( "buffer(cascading.operation.Buffer buffer)" ).last()
      .addMethod( "outgoing(cascading.tuple.Fields outgoingSelector)" ).last()
      .endBlock() // buffer

      .endBlock() // every

      .addMethod( "completeGroupBy()" ).last()
      .endBlock(); // groupBy

    branch = branch
      .addBlockReference( "GroupBy", "groupBy(cascading.tuple.Fields groupFields, cascading.tuple.Fields sortFields)" ).any( GROUP );

    branch = branch
      .addMethod( "checkpoint(String name)" )
      .withDocumentation()
      .addContent( "Append a new Checkpoint pipe to the current branch with the given name.\n" )
      .addContent( "@see " + Checkpoint.class.getName() )
      .finish()
      .any()
      .addMethod( "checkpoint()" )
      .withDocumentation()
      .addContent( "Append a new Checkpoint pipe to the current branch.\n" )
      .addContent( "@see " + Checkpoint.class.getName() )
      .finish()
      .any(); // not required to be named

    branch = addSubTypeBlocks( branch, Reflection.loadClass( SubAssembly.class.getName() ), false, false, PIPE_FACTORY, true, Reflection.loadClass( Pipe.class.getName() ) ); // sub-assemblies

    branch
      .addMethod( "completeBranch()" )
      .withDocumentation( "Complete the current branch and return the current tail." )
      .last( Pipe.class )
      .endBlock(); // branch

    branch = builder.startBlock( "Group", "continueBranch(cascading.pipe.GroupBy groupBy)" )
      .withDocumentation( "Continue a branch from the given groupBy." )
      .any()
      .addBlockReference( "Every", "every(cascading.tuple.Fields argumentSelector)" ).any()
      .addBlockReference( "Each", "each(cascading.tuple.Fields argumentSelector)" ).any();

    branch = addSubTypeBlocks( branch, Reflection.loadClass( SubAssembly.class.getName() ), false, true, PIPE_FACTORY, true, Reflection.loadClass( Pipe.class.getName() ) );

    branch.addMethod( "completeBranch()" )
      .withDocumentation( "Complete the current branch and return the current tail." )
      .last( Pipe.class )
      .endBlock(); // branch

    builder.addBlockReference( "Group", "continueBranch(cascading.pipe.CoGroup coGroup)" )
      .withDocumentation( "Continue a branch from the given coGroup." )
      .any();
    builder.addBlockReference( "Branch", "continueBranch(cascading.pipe.Pipe pipe)" )
      .withDocumentation( "Continue a branch from the given pipe." )
      .any();
    builder.addBlockReference( "Branch", "continueBranch(String name)" )
      .withDocumentation( "Continue a branch with the given name." )
      .any();

    builder.addBlockReference( "Group", "continueBranch(String name, cascading.pipe.GroupBy groupBy)" )
      .withDocumentation( "Continue a branch from the given groupBy with the new given name." )
      .any();
    builder.addBlockReference( "Group", "continueBranch(String name, cascading.pipe.CoGroup coGroup)" )
      .withDocumentation( "Continue a branch from the given coGroup with the new given name." )
      .any();
    builder.addBlockReference( "Branch", "continueBranch(String name, cascading.pipe.Pipe pipe)" )
      .withDocumentation( "Continue a branch from the given pipe with the new given name." )
      .any();

    addPipeBranchBuilderType( builder, "CoGroup", Reflection.loadClass( CoGroup.class.getName() ), COGROUP, false, FACTORY );
    addPipeBranchBuilderType( builder, "HashJoin", Reflection.loadClass( HashJoin.class.getName() ), HASH_JOIN, false, FACTORY );
    addPipeBranchBuilderType( builder, "GroupByMerge", Reflection.loadClass( GroupBy.class.getName() ), GROUP_MERGE, true, FACTORY );
    addPipeBranchBuilderType( builder, "Merge", Reflection.loadClass( Merge.class.getName() ), MERGE, true, FACTORY );
    }
  }
