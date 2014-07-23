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

import cascading.fluid.generator.util.ParameterGraphs;
import cascading.fluid.generator.util.Prefix;
import cascading.fluid.generator.util.Text;
import cascading.fluid.generator.util.Types;
import com.google.common.collect.Iterables;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graphs;
import org.jgrapht.alg.FloydWarshallShortestPaths;
import org.jgrapht.event.TraversalListenerAdapter;
import org.jgrapht.event.VertexTraversalEvent;
import org.jgrapht.traverse.DepthFirstIterator;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unquietcode.tools.flapi.Descriptor;
import unquietcode.tools.flapi.Flapi;
import unquietcode.tools.flapi.builder.Block.BlockBuilder;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder_m1_m4_m5;
import unquietcode.tools.flapi.builder.Method.MethodBuilder_m7_m8_m9_m10_m11;
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
  public static final int GROUP_MERGE = 4;

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

  protected DescriptorBuilder.$<Void> getBuilder()
    {
    if( LOG.isDebugEnabled() )
      return Flapi.builder( methodLogger );

    return Flapi.builder();
    }

  protected abstract String getFactoryClass();

//  protected <T> BlockBuilder addBuilderBlock( BlockBuilder<DescriptorBuilder_m1_m4_m5<Void>> builder, Class<T> type, final boolean isFactory, int group )
//    {
//    String typeName = type.getSimpleName();
//    MethodBuilder_m7_m8_m9_m10_m11 tmp1 = builder.startBlock( typeName, Text.toFirstLower( typeName ) + "()" );
//    BlockBuilder block = (BlockBuilder) ( isFactory ? tmp1.last() : tmp1.after( group ).last() );
//
//    return (BlockBuilder) addSubTypeBlocks( block, type, isFactory );
//    }

  protected <T> DescriptorBuilder_m1_m4_m5 addBuilderBlock( DescriptorBuilder_m1_m4_m5 builder, Class<T> type, final boolean isFactory, int group )
    {
    String typeName = type.getSimpleName();
    MethodBuilder_m7_m8_m9_m10_m11 tmp1 = builder.startBlock( typeName, Text.toFirstLower( typeName ) + "()" );
    BlockBuilder block = (BlockBuilder) ( isFactory ? tmp1.last() : tmp1.after( group ).last() );

    return (DescriptorBuilder_m1_m4_m5) addSubTypeBlocks( block, type, isFactory ).endBlock();
    }

  protected <T> BlockBuilder addSubTypeBlocks( BlockBuilder block, Class<T> type, final boolean isFactory, Class... startAfter )
    {
    Map<Class<? extends T>, Set<Constructor>> constructorMap = Types.getAllInstantiable( reflections, type );

    for( final Class<? extends T> subType : constructorMap.keySet() )
      {
      LOG.info( "adding {}: {}", type.getSimpleName(), subType.getName() );

      final Set<Constructor> constructors = constructorMap.get( subType );

      final String operationName = subType.getSimpleName();
      String methodName = isFactory ? operationName : Text.toFirstLower( operationName ); // Factory methods have upper first letter
      MethodBuilder_m7_m8_m9_m10_m11 tmp = block.startBlock( operationName, methodName + "()" );

      block = (BlockBuilder) ( (MethodBuilder_m7_m8_m9_m10_m11<DescriptorBuilder_m1_m4_m5>) tmp ).addAnnotation( METHOD_ANNOTATION )
        .withClassParam( "factory" ).havingValue( getFactoryClass() )
        .withClassParam( "creates" ).havingValue( subType )
        .finish().last();

      final BlockBuilder[] blockBuilder = {
        block
      };

      final DirectedGraph<Prefix<String, String, Class>, Integer> graph = ParameterGraphs.createParameterGraph( constructors, true, startAfter );
      final FloydWarshallShortestPaths<Prefix<String, String, Class>, Integer> shortestPaths = new FloydWarshallShortestPaths<Prefix<String, String, Class>, Integer>( graph );

      ParameterGraphs.writeDOT( subType.getName(), graph );

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
          blockBuilder[ 0 ] = (BlockBuilder) ( (BlockBuilder<BlockBuilder<DescriptorBuilder_m1_m4_m5<Void>>>) blockBuilder[ 0 ] )
            .startBlock( methodSignature )
            .last()
            .addMethod( "end()" )
            .last( subType );
          }
        else if( hasTerminalPath )
          {
          blockBuilder[ 0 ] = (BlockBuilder) ( (BlockBuilder<BlockBuilder<DescriptorBuilder_m1_m4_m5<Void>>>) blockBuilder[ 0 ] )
            .startBlock( methodSignature )
            .last()
            .addMethod( "end()" )
            .last();
          }
        else
          {
          blockBuilder[ 0 ] = (BlockBuilder) blockBuilder[ 0 ].startBlock( methodSignature ).last();
          }
        }

      @Override
      public void vertexFinished( VertexTraversalEvent<Prefix<String, String, Class>> event )
        {
        Prefix<String, String, Class> vertex = event.getVertex();

        if( vertex == BEGIN || vertex == END )
          return;

        LOG.info( "{} - closing property: {}", (int) shortestPaths.shortestDistance( BEGIN, vertex ), vertex.getLhs() );

        blockBuilder[ 0 ] = (BlockBuilder) blockBuilder[ 0 ].endBlock();
        }
      };

      DepthFirstIterator<Prefix<String, String, Class>, Integer> iterator = new DepthFirstIterator<Prefix<String, String, Class>, Integer>( graph, BEGIN );

      iterator.addTraversalListener( listener );

      while( iterator.hasNext() )
        iterator.next();

      block = (BlockBuilder<DescriptorBuilder_m1_m4_m5<Void>>) blockBuilder[ 0 ].endBlock();
      }

    return block;
    }

  private String createMethodSignature( Prefix<String, String, Class> pair )
    {
    Class parameterType = pair.getRhs();
    String property = pair.getLhs();

    String methodSignature;

    if( parameterType.isArray() )
      methodSignature = property + "(" + parameterType.getComponentType().getName() + "... " + property + ")";
    else
      methodSignature = property + "(" + parameterType.getName() + " " + property + ")";

    return methodSignature;
    }
  }
