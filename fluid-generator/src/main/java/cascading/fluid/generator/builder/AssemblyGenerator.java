/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

import cascading.fluid.generator.util.Reflection;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unquietcode.tools.flapi.ClassReference;
import unquietcode.tools.flapi.Descriptor;
import unquietcode.tools.flapi.builder.Block.BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f;

/**
 *
 */
public class AssemblyGenerator extends Generator
  {
  private static final Logger LOG = LoggerFactory.getLogger( AssemblyGenerator.class );

  public AssemblyGenerator()
    {
    }

  public AssemblyGenerator( String... packages )
    {
    super( packages );
    }

  public void createAssemblyBuilder( String targetPath )
    {
    DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> builder = getBuilder()
      .setPackage( "cascading.fluid.api.assembly" )
      .setDescriptorName( "Assembly" )
      .setStartingMethodName( "startAssembly" );

    builder = addBranchBlock( builder );

    builder = builder.addMethod( "completeAssembly()" ).last( Pipe[].class ); // tails

    Descriptor build = builder.enableCondensedClassNames().build();

    writeBuilder( targetPath, build );
    }

  private DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> addBranchBlock( DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> builder )
    {
    BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>> branch = builder
      .startBlock( "Branch", "startBranch(String name)" ).any();

    branch = branch
      .addMethod( "pipe(String name)" ).any()
      .addMethod( "pipe(String name, cascading.pipe.Pipe previous )" ).any();

    branch = branch
      .startBlock( "Each", "each(cascading.tuple.Fields argumentSelector)" )
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( PIPE_FACTORY ) )
      .withParameter( "creates", Each.class )
      .withParameter( "method", "each(cascading.tuple.Fields argumentSelector)" )
      .finish()
      .any( EACH )

      .startBlock( "function(cascading.operation.Function function)" ).last()
      .addMethod( "outgoing(cascading.tuple.Fields outgoingSelector)" ).last()
      .endBlock() // function

      .addMethod( "filter(cascading.operation.Filter filter)" ).last() // filter
      .endBlock(); // each

    branch = branch
      .startBlock( "GroupBy", "groupBy(cascading.tuple.Fields groupFields)" )
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( PIPE_FACTORY ) )
      .withParameter( "creates", GroupBy.class )
      .withParameter( "createOnNext", true )
      .withParameter( "method", "groupBy(cascading.tuple.Fields groupFields)" )
      .finish()
      .any( GROUP )

      .startBlock( "Every", "every(cascading.tuple.Fields argumentSelector)" )//.after( GROUP )
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

    branch = addSubTypeBlocks( branch, Reflection.loadClass( SubAssembly.class.getName() ), false, false, PIPE_FACTORY, Reflection.loadClass( Pipe.class.getName() ) ); // sub-assemblies

    builder = branch
      .addMethod( "completeBranch()" ).last( Pipe.class )
      .endBlock(); // branch

    branch = builder.startBlock( "Group", "continueBranch(cascading.pipe.GroupBy groupBy)" ).any()
      .addBlockReference( "Every", "every(cascading.tuple.Fields argumentSelector)" ).any()
      .addBlockReference( "Each", "each(cascading.tuple.Fields argumentSelector)" ).any();

    branch = addSubTypeBlocks( branch, Reflection.loadClass( SubAssembly.class.getName() ), false, true, PIPE_FACTORY, Reflection.loadClass( Pipe.class.getName() ) );

    builder = branch.addMethod( "completeBranch()" ).last( Pipe.class )
      .endBlock(); // branch

    builder.addBlockReference( "Group", "continueBranch(cascading.pipe.CoGroup coGroup)" ).any();
    builder.addBlockReference( "Branch", "continueBranch(cascading.pipe.Pipe pipe)" ).any();

    builder.addBlockReference( "Group", "continueBranch(String name, cascading.pipe.GroupBy groupby)" ).any();
    builder.addBlockReference( "Group", "continueBranch(String name, cascading.pipe.CoGroup coGroup)" ).any();
    builder.addBlockReference( "Branch", "continueBranch(String name, cascading.pipe.Pipe pipe)" ).any();

    builder = addPipeBranchBuilderType( builder, "CoGroup", Reflection.loadClass( CoGroup.class.getName() ), COGROUP, false, FACTORY );
    builder = addPipeBranchBuilderType( builder, "HashJoin", Reflection.loadClass( HashJoin.class.getName() ), HASH_JOIN, false, FACTORY );
    builder = addPipeBranchBuilderType( builder, "GroupByMerge", Reflection.loadClass( GroupBy.class.getName() ), GROUP_MERGE, true, FACTORY );
    builder = addPipeBranchBuilderType( builder, "Merge", Reflection.loadClass( Merge.class.getName() ), MERGE, true, FACTORY );

    return builder;
    }

  }
