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

import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import unquietcode.tools.flapi.ClassReference;
import unquietcode.tools.flapi.Descriptor;
import unquietcode.tools.flapi.builder.Block.BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f;
import unquietcode.tools.flapi.builder.Method.MethodBuilder_2m12_4f_2m13_4f_2m14_4f_2m15_4f_2m16_4f_2m17_4f_2m18_4f;

/**
 *
 */
public class AssemblyGenerator extends Generator
  {
  public AssemblyGenerator()
    {
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
      .finish()
      .any( GROUP )

      .startBlock( "Every", "every(cascading.tuple.Fields argumentSelector)" )//.after( GROUP )
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( PIPE_FACTORY ) )
      .withParameter( "creates", Every.class )
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

    branch = addSubTypeBlocks( branch, SubAssembly.class, false, Pipe.class );

    builder = branch
      .addMethod( "completeBranch()" ).last( Pipe.class )
      .endBlock(); // branch

    builder = builder
      .startBlock( "GroupByMerge", "groupByMerge(cascading.tuple.Fields groupFields, cascading.pipe.Pipe[] pipes)" ).any( GROUP_MERGE )

      .addBlockReference( "Every", "every(cascading.tuple.Fields argumentSelector)" ).any( EVERY )

      .addBlockReference( "Each", "each(cascading.tuple.Fields argumentSelector)" ).any( EACH )

      .addMethod( "completeBranch()" ).last( Pipe.class )
      .endBlock(); // groupByMerge

    builder = builder
      .addBlockReference( "GroupByMerge", "groupByMerge(cascading.tuple.Fields groupFields, cascading.tuple.Fields sortFields, cascading.pipe.Pipe[] pipes)" ).any( GROUP_MERGE );

    return builder;
    }

  @Override
  protected String getFactoryClass()
    {
    return PIPE_FACTORY;
    }
  }
