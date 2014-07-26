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

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import cascading.fluid.generator.util.ParameterGraphs;
import cascading.fluid.generator.util.Prefix;
import cascading.fluid.generator.util.Text;
import cascading.fluid.generator.util.Types;
import cascading.pipe.Splice;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graphs;
import org.jgrapht.alg.FloydWarshallShortestPaths;
import org.jgrapht.event.TraversalListenerAdapter;
import org.jgrapht.event.VertexTraversalEvent;
import org.jgrapht.traverse.DepthFirstIterator;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unquietcode.tools.flapi.ClassReference;
import unquietcode.tools.flapi.Descriptor;
import unquietcode.tools.flapi.Flapi;
import unquietcode.tools.flapi.builder.Block.BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f;
import unquietcode.tools.flapi.builder.Method.MethodBuilder_2m12_4f_2m13_4f_2m14_4f_2m15_4f_2m16_4f_2m17_4f_2m18_4f;
import unquietcode.tools.flapi.runtime.MethodLogger;

import static cascading.fluid.generator.util.ParameterGraphs.BEGIN;
import static cascading.fluid.generator.util.ParameterGraphs.END;

/**
 *
 */
public abstract class Generator
  {
  private static final Logger LOG = LoggerFactory.getLogger( Generator.class );

  public static final String METHOD_ANNOTATION = "cascading.fluid.factory.MethodMeta";
  public static final String FACTORY = "cascading.fluid.factory.Factory";
  public static final String PIPE_FACTORY = "cascading.fluid.factory.PipeFactory";

  public static final int START = 0;
  public static final int GROUP = 1;
  public static final int EACH = 2;
  public static final int EVERY = 3;
  public static final int COGROUP = 4;
  public static final int GROUP_MERGE = 5;
  public static final int MERGE = 6;
  public static final int HASH_JOIN = 7;

  protected static MethodLogger methodLogger = MethodLogger.from( System.out );
  protected static Reflections reflections;

  protected Generator()
    {
    reflections = new Reflections( "cascading" );
    }

  protected void writeBuilder( String targetPath, Descriptor build )
    {
    new File( targetPath ).mkdirs();

    build.writeToFolder( targetPath );
    }

  protected DescriptorBuilder.Start getBuilder()
    {
    if( LOG.isDebugEnabled() )
      return Flapi.builder( methodLogger );

    return Flapi.builder();
    }

  protected <T> DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> addBuilderBlock( DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> builder, Class<T> type, final boolean isFactory, int group, String factoryClass )
    {
    String typeName = type.getSimpleName();
    MethodBuilder_2m12_4f_2m13_4f_2m14_4f_2m15_4f_2m16_4f_2m17_4f_2m18_4f tmp1 = builder
      .startBlock( typeName, Text.toFirstLower( typeName ) + "()" );

    BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f block = (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f) ( isFactory ? tmp1.last() : tmp1.after( group ).last() );

    return (DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>) addSubTypeBlocks( block, type, isFactory, false, factoryClass )
      .endBlock();
    }

  protected <T> BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f addSubTypeBlocks( BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f block, Class<T> type, final boolean isFactory, boolean addReference, String factoryClass, Class... startsWithExclusive )
    {
    Map<Class<? extends T>, Set<Constructor>> constructorMap = Types.getAllInstantiableSubTypes( reflections, type );

    for( final Class<? extends T> subType : constructorMap.keySet() )
      {
      Set<Constructor> constructors = constructorMap.get( subType );

      LOG.info( "adding block {}: subtype: {}", type.getSimpleName(), subType.getName() );

      if( constructors.size() > 1 )
        block = addTypeBuilderBlock( block, isFactory, subType, constructors, addReference, factoryClass, startsWithExclusive );
      else
        block = addTypeBuilderMethod( block, isFactory, subType, constructors, addReference, factoryClass, startsWithExclusive );
      }

    return block;
    }

  protected DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> addPipeBranchBuilderType(
    DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> builder,
    String operationName,
    Class<? extends Splice> splice, int groupID, boolean isMerge, String factoryClass )
    {
    Set<Constructor> constructors;

    if( isMerge )
      constructors = Types.getConstructorsWithMultiplePipes( splice );
    else
      constructors = Types.getInstantiableConstructors( splice );

    builder = addPipeTypeBuilderBlock( builder, splice, constructors, operationName, groupID, factoryClass );

    return builder;
    }

  protected <T> DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> addPipeTypeBuilderBlock(
    DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> block,
    final Class<? extends T> type,
    Set<Constructor> constructors,
    String operationName,
    int groupID, String factoryClass,
    Class... startsWithExclusive )
    {
    String startMethod = "start" + operationName + "()";
    String endMethod = "create" + operationName + "()";

    LOG.info( "to type: {}, adding methodName: {}", type.getName(), startMethod );

    // startBlock
    BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>> tmp = block
      .startBlock( operationName, startMethod )
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( factoryClass ) )
      .withParameter( "creates", type )
      .finish()
      .any( groupID );

    BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f blockBuilder = generateBlock( tmp, true, type, constructors, endMethod, startsWithExclusive );

    return ( (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>>) blockBuilder )
//      .addMethod( endMethod ).last( type )
      .endBlock();
    }

  protected <T> BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f addTypeBuilderMethod( BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f block, final boolean isFactory, final Class<? extends T> type, Set<Constructor> constructors, boolean addReference, String factoryClass, Class... startsWithExclusive )
    {
    final DirectedGraph<Prefix<String, String, Class>, Integer> graph = ParameterGraphs.createParameterGraph( constructors, true, startsWithExclusive );

    final String operationName = type.getSimpleName();
    String methodName = ( isFactory ? operationName : Text.toFirstLower( operationName ) ); // Factory methods have upper first letter

    methodName = createMethodSignature( methodName, graph );

    LOG.info( "to type: {}, adding methodName: {}", type.getName(), methodName );

    // method
    MethodBuilder_2m12_4f_2m13_4f_2m14_4f_2m15_4f_2m16_4f_2m17_4f_2m18_4f<BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>>>> tmp = ( (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>>>) block )
      .addMethod( methodName )
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( factoryClass ) )
      .withParameter( "creates", type )
      .finish();

    block = isFactory ? tmp.last( type ) : tmp.any(); // allow subsequent pipe elements

    return block;
    }

  protected <T> BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f addTypeBuilderBlock( BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f block, final boolean isFactory, final Class<? extends T> type, Set<Constructor> constructors, boolean addReference, String factoryClass, Class... startsWithExclusive )
    {
    final String operationName = type.getSimpleName();
    String methodName = ( isFactory ? operationName : Text.toFirstLower( operationName ) ) + "()"; // Factory methods have upper first letter

    LOG.info( "to type: {}, adding methodName: {}", type.getName(), methodName );

    if( addReference )
      return (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f) block.addBlockReference( operationName, methodName ).any();

    // startBlock
    MethodBuilder_2m12_4f_2m13_4f_2m14_4f_2m15_4f_2m16_4f_2m17_4f_2m18_4f<BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>>>>> tmp = ( (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>>>) block )
      .startBlock( operationName, methodName )
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( factoryClass ) )
      .withParameter( "creates", type )
      .finish();

    block = isFactory ? tmp.last() : tmp.any(); // allow subsequent pipe elements

    BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f blockBuilder = generateBlock( block, isFactory, type, constructors, "end()", startsWithExclusive );

    // endBlock
    block = (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>>) blockBuilder
      .endBlock();

    return block;
    }

  private <T> BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f generateBlock( BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f block, final boolean isFactory, final Class<? extends T> type, Set<Constructor> constructors, final String endMethod, Class[] startsWithExclusive )
    {
    final BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f[] blockBuilder = {
      block
    };

    final DirectedGraph<Prefix<String, String, Class>, Integer> graph = ParameterGraphs.createParameterGraph( constructors, true, startsWithExclusive );
    final FloydWarshallShortestPaths<Prefix<String, String, Class>, Integer> shortestPaths = new FloydWarshallShortestPaths<Prefix<String, String, Class>, Integer>( graph );

    ParameterGraphs.writeDOT( type.getName(), graph );

    TraversalListenerAdapter<Prefix<String, String, Class>, Integer> listener = new TraversalListenerAdapter<Prefix<String, String, Class>, Integer>()
    {
    @Override
    public void vertexTraversed( VertexTraversalEvent<Prefix<String, String, Class>> event )
      {
      Prefix<String, String, Class> vertex = event.getVertex();

      if( vertex == BEGIN || vertex == END )
        return;

      Integer current = Iterables.getFirst( new TreeSet<Integer>( graph.incomingEdgesOf( vertex ) ), 0 );
      Integer prior = Iterables.getFirst( new TreeSet<Integer>( graph.incomingEdgesOf( graph.getEdgeSource( current ) ) ), null );

      int depth = (int) shortestPaths.shortestDistance( BEGIN, vertex );

      String methodSignature = createMethodSignature( vertex );

      LOG.info( "{} - opening property: {}, creating method: {}, group: {}, prior: {}", depth, vertex.getLhs(), methodSignature, depth, prior != null );

      int outDegree = graph.outDegreeOf( vertex );
      boolean hasTerminalPath = Graphs.successorListOf( graph, vertex ).contains( END );
      boolean isTerminal = outDegree == 1 && hasTerminalPath;

      if( hasTerminalPath && isFactory )
        {
        blockBuilder[ 0 ] = (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f) ( (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>>>) blockBuilder[ 0 ] )
          .startBlock( methodSignature )
          .last()
          .addMethod( endMethod )
          .last( type );
        }
      else if( hasTerminalPath )
        {
        blockBuilder[ 0 ] = (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f) ( (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f<DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void>>>) blockBuilder[ 0 ] )
          .startBlock( methodSignature )
          .last()
          .addMethod( endMethod )
          .last();
        }
      else
        {
        blockBuilder[ 0 ] = (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f) blockBuilder[ 0 ]
          .startBlock( methodSignature ).last();
        }
      }

    @Override
    public void vertexFinished( VertexTraversalEvent<Prefix<String, String, Class>> event )
      {
      Prefix<String, String, Class> vertex = event.getVertex();

      if( vertex == BEGIN || vertex == END )
        return;

      LOG.info( "{} - closing property: {}", (int) shortestPaths.shortestDistance( BEGIN, vertex ), vertex.getLhs() );

      blockBuilder[ 0 ] = (BlockBuilder_2m1_4f_2m2_4f_2m3_4f_2m10_4f_2m11_4f) blockBuilder[ 0 ]
        .endBlock();
      }
    };

    DepthFirstIterator<Prefix<String, String, Class>, Integer> iterator = new DepthFirstIterator<Prefix<String, String, Class>, Integer>( graph, BEGIN );

    iterator.addTraversalListener( listener );

    while( iterator.hasNext() )
      iterator.next();

    return blockBuilder[ 0 ];
    }

  private String createMethodSignature( String methodName, DirectedGraph<Prefix<String, String, Class>, Integer> graph )
    {
    DepthFirstIterator<Prefix<String, String, Class>, Integer> iterator = new DepthFirstIterator<Prefix<String, String, Class>, Integer>( graph, BEGIN );

    String args = Joiner.on( "," ).skipNulls().join( Iterators.transform( iterator, new Function<Prefix<String, String, Class>, Object>()
    {
    @Nullable
    @Override
    public Object apply( @Nullable Prefix<String, String, Class> prefix )
      {
      if( prefix.getPair() == null )
        return null;

      return createMethodArg( prefix );
      }
    } ) );

    return methodName + "(" + args + ")";
    }

  private String createMethodSignature( Prefix<String, String, Class> pair )
    {
    return pair.getLhs() + "(" + createMethodArg( pair ) + ")";
    }

  private String createMethodArg( Prefix<String, String, Class> pair )
    {
    String property = pair.getLhs();
    Class parameterType = pair.getRhs();

    if( parameterType.isArray() )
    // return parameterType.getComponentType().getName() + "... " + property;
      return parameterType.getComponentType().getName() + "[] " + property; // TODO: enable vargs
    else
      return parameterType.getName() + " " + property;
    }
  }
