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

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;

import cascading.fluid.generator.javadocs.DocsHelper;
import cascading.fluid.generator.util.ParameterGraphs;
import cascading.fluid.generator.util.StringClassPrefix;
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
import unquietcode.tools.flapi.builder.Block.BlockBuilder;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder;
import unquietcode.tools.flapi.builder.Method.MethodBuilder;
import unquietcode.tools.flapi.builder.Method.Sa255b39f2e2c8808a7291906605de632;
import unquietcode.tools.flapi.generator.naming.HashedNameGenerator;
import unquietcode.tools.flapi.runtime.MethodLogger;

import static cascading.fluid.generator.util.ParameterGraphs.BEGIN;
import static cascading.fluid.generator.util.ParameterGraphs.END;


public abstract class Generator
  {
  protected static final Logger LOG = LoggerFactory.getLogger( Generator.class );

  public static final String DEFAULT_PACKAGE = "cascading";

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
  public static final int AGGREGATE_BY = 8;

  protected static MethodLogger methodLogger = MethodLogger.from( System.out );

  private boolean enableVarArgs = true; // only used if constructor parameter was declared with varargs
  private boolean includeCascading = true; // include cascading types on the classpath

  protected final Reflections reflections;
  protected final DocsHelper documentationHelper;

  protected Generator( DocsHelper documentationHelper )
    {
    this(documentationHelper, new Reflections( DEFAULT_PACKAGE ));
    }

  protected Generator( DocsHelper documentationHelper, Reflections reflections )
    {
    this.documentationHelper = documentationHelper;
    this.reflections = Objects.requireNonNull(reflections);
    }

  public boolean isEnableVarArgs()
    {
    return enableVarArgs;
    }

  public void setEnableVarArgs( boolean enableVarArgs )
    {
    this.enableVarArgs = enableVarArgs;
    }

  public boolean isIncludeCascading()
    {
    return includeCascading;
    }

  public void setIncludeCascading( boolean includeCascading )
    {
    this.includeCascading = includeCascading;
    }

  public void generate( String targetPath )
    {
    DescriptorBuilder.Start<?> builder = generateInternal( targetPath );
    completeAndWriteBuilder( targetPath, builder );
    }

  protected abstract DescriptorBuilder.Start<?> generateInternal( String targetPath );

  private void completeAndWriteBuilder( String targetPath, DescriptorBuilder.Start<?> builder )
    {
    Descriptor descriptor = builder
      .useCustomNameGenerator( new HashedNameGenerator() )
      .disableTimestamps()
      .build();

    new File( targetPath ).mkdirs();

    descriptor.writeToFolder( targetPath );
    }

  protected DescriptorBuilder.Start<?> getBuilder()
    {
    if( LOG.isDebugEnabled() )
      return Flapi.builder( methodLogger );

    return Flapi.builder();
    }

  protected <T> void addBuilderBlock( DescriptorBuilder.Start<?> builder, Class<T> type, final boolean isFactory, Integer group, String factoryClass, boolean allConstructors )
    {
    String typeName = type.getSimpleName();
    String methodSignature = Text.toFirstLower( typeName ) + "()";

    // skip when empty
    if (!atLeastOne( type, allConstructors )) {
      return;
    }

    MethodBuilder.Start<?> tmp1 = builder
      .startBlock( typeName, methodSignature );

    // lookup and attach docs
    documentationHelper.addDocumentation( tmp1, type, methodSignature );

    BlockBuilder.Start<?> block = (BlockBuilder.Start<?>) ( isFactory || group == null ? tmp1.last() : tmp1.after( group ).last() );

    addSubTypeBlocks( block, type, isFactory, false, factoryClass, allConstructors )
      .endBlock();
    }

  protected <T> BlockBuilder.Start<?> addSubTypeBlocks( BlockBuilder.Start<?> block, Class<T> type, final boolean isFactory, boolean addReference, String factoryClass, boolean allConstructors, Class... startsWithExclusive )
    {
    Map<Class<? extends T>, Set<Constructor>> constructorMap = Types.getAllInstantiableSubTypes( reflections, type, allConstructors );
    boolean atLeastOne = false;

    for( final Class<? extends T> subType : constructorMap.keySet() )
      {

      // skip cascading classes if desired
      if (!includeCascading && subType.getPackage().getName().startsWith( DEFAULT_PACKAGE ))
        {
        LOG.debug( "skipping cascading type '{}'", subType );
        continue;
        }

      atLeastOne = true;
      Set<Constructor> constructors = constructorMap.get( subType );

      LOG.debug( "adding block {}: subtype: {}, constructors: {}", type.getSimpleName(), subType.getName(), constructors.size() );

      if( constructors.size() > 1 )
        block = addTypeBuilderBlock( block, isFactory, subType, constructors, addReference, factoryClass, startsWithExclusive );
      else
        block = addTypeBuilderMethod( block, isFactory, subType, constructors, factoryClass, startsWithExclusive );
      }

    // if this is reached we need to defend against empty blocks
    // (so ideally don't call this method if there aren't any constructors)
    if ( !atLeastOne )
      {
      block.addMethod( "done()" ).last();
      }

    return block;
    }

  protected <T> boolean atLeastOne( Class<T> type, boolean allConstructors )
    {
    Map<Class<? extends T>, Set<Constructor>> constructorMap = Types.getAllInstantiableSubTypes( reflections, type, allConstructors );

    for( final Class<? extends T> subType : constructorMap.keySet() )
      {

      if (!includeCascading && subType.getPackage().getName().startsWith( DEFAULT_PACKAGE ))
        {
        LOG.debug( "skipping cascading type '{}'", subType );
        continue;
        }

      return true;
      }

    return false;
    }

  protected void addPipeBranchBuilderType( DescriptorBuilder.Start<?> builder, String operationName, Class<? extends Splice> splice, Integer groupID, boolean isMerge, String factoryClass )
    {
    Set<Constructor> constructors;

    if( isMerge )
      constructors = Types.getConstructorsWithMultiplePipes( splice );
    else
      constructors = Types.getInstantiableConstructors( splice );

    addPipeTypeBuilderBlock( builder, splice, constructors, operationName, groupID, factoryClass );
    }

  protected <T> void addPipeTypeBuilderBlock( DescriptorBuilder.Start<?> block, final Class<? extends T> type, Set<Constructor> constructors, String operationName, Integer groupID, String factoryClass, Class... startsWithExclusive )
    {
    String startMethod = "start" + operationName + "()";
    String endMethod = "create" + operationName + "()";

    DirectedGraph<StringClassPrefix, Integer> parameterGraph = ParameterGraphs.createParameterGraph( constructors, true, startsWithExclusive );

    if( parameterGraph.vertexSet().size() == 2 ) // has no parameters
      {
      LOG.debug( "on type: {}, skipping methodName: {}", type.getName(), endMethod );
      return;
      }

    LOG.debug( "to type: {}, adding methodName: {}", type.getName(), startMethod );

    // startBlock
    Sa255b39f2e2c8808a7291906605de632<?> method = block
      .startBlock( operationName, startMethod )
      .withDocumentation()
      .addContent( "Create a new " + type.getSimpleName() + " pipe to the current branch with the given groupFields.\n" )
      .addContent( "@see " + type.getName() )
      .finish()
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( factoryClass ) )
      .withParameter( "creates", type )
      .withParameter( "method", startMethod )
      .finish();

    BlockBuilder.Start<?> tmp = (BlockBuilder.Start<?>) (groupID != null ? method.any( groupID ) : method.any());

    BlockBuilder.Start<?> blockBuilder = generateBlock( tmp, true, type, endMethod, parameterGraph );

    blockBuilder.endBlock();
// .addMethod( endMethod ).last( type )
    }

  protected <T> BlockBuilder.Start<?> addTypeBuilderMethod( BlockBuilder.Start<?> block, final boolean isFactory, final Class<? extends T> type, Set<Constructor> constructors, String factoryClass, Class... startsWithExclusive )
    {
    final DirectedGraph<StringClassPrefix, Integer> graph = ParameterGraphs.createParameterGraph( constructors, true, startsWithExclusive );

    final String operationName = type.getSimpleName();
    String methodName = ( isFactory ? operationName : Text.toFirstLower( operationName ) ); // Factory methods have upper first letter
    String methodSignature = createMethodSignature( methodName, true, graph );

    LOG.debug( "to type: {}, adding methodName: {}, params: {}", type.getName(), methodSignature, graph.vertexSet().size() - 2 );

    // method
    MethodBuilder.Start<?> tmp = block
      .addMethod( methodSignature )
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( factoryClass ) )
      .withParameter( "creates", type )
      .withParameter( "method", methodSignature )
      .finish();

    // lookup and attach docs
    String flatSignature = createMethodSignature( methodName, false, graph );
    documentationHelper.addDocumentation( tmp, type, flatSignature );

    block = (BlockBuilder.Start<?>) ( isFactory ? tmp.last( type ) : tmp.any() ); // allow subsequent pipe elements

    return block;
    }

  protected <T> BlockBuilder.Start<?> addTypeBuilderBlock( BlockBuilder.Start<?> block, final boolean isFactory, final Class<? extends T> type, Set<Constructor> constructors, boolean addReference, String factoryClass, Class... startsWithExclusive )
    {
    final String operationName = type.getSimpleName();
    String methodName = ( isFactory ? operationName : Text.toFirstLower( operationName ) ) + "()"; // Factory methods have upper first letter

    DirectedGraph<StringClassPrefix, Integer> parameterGraph = ParameterGraphs.createParameterGraph( constructors, true, startsWithExclusive );

    if( parameterGraph.vertexSet().size() == 2 ) // has no parameters
      {
      LOG.debug( "on type: {}, skipping methodName: {}", type.getName(), methodName );
      return block;
      }

    LOG.debug( "to type: {}, adding methodName: {}", type.getName(), methodName );

    if( addReference )
      return block.addBlockReference( operationName, methodName ).any();

    // startBlock
    MethodBuilder.Start<?> tmp = block
      .startBlock( operationName, methodName )
      .addAnnotation( METHOD_ANNOTATION )
      .withParameter( "factory", new ClassReference( factoryClass ) )
      .withParameter( "creates", type )
      .withParameter( "method", methodName )
      .finish();

    block = (BlockBuilder.Start<?>) ( isFactory ? tmp.last() : tmp.any() ); // allow subsequent pipe elements
    documentationHelper.addDocumentation( tmp, type, methodName );

    BlockBuilder.Start<?> blockBuilder = generateBlock( block, isFactory, type, "end()", parameterGraph );

    // endBlock
    block = (BlockBuilder.Start<?>) blockBuilder.endBlock();

    return block;
    }

  private <T> BlockBuilder.Start<?> generateBlock( BlockBuilder.Start<?> block, final boolean isFactory, final Class<? extends T> type, final String endMethod, final DirectedGraph<StringClassPrefix, Integer> graph )
    {
    final BlockBuilder.Start<?>[] blockBuilder = {
      block
    };

    final FloydWarshallShortestPaths<StringClassPrefix, Integer> shortestPaths = new FloydWarshallShortestPaths<StringClassPrefix, Integer>( graph );

    ParameterGraphs.writeDOT( type.getName(), graph );

    TraversalListenerAdapter<StringClassPrefix, Integer> listener = new TraversalListenerAdapter<StringClassPrefix, Integer>()
    {
    @Override
    public void vertexTraversed( VertexTraversalEvent<StringClassPrefix> event )
      {
      StringClassPrefix vertex = event.getVertex();

      if( vertex == BEGIN || vertex == END )
        return;

      Integer current = Iterables.getFirst( new TreeSet<Integer>( graph.incomingEdgesOf( vertex ) ), 0 );
      Integer prior = Iterables.getFirst( new TreeSet<Integer>( graph.incomingEdgesOf( graph.getEdgeSource( current ) ) ), null );

      int depth = (int) shortestPaths.shortestDistance( BEGIN, vertex );

      String methodSignature = createMethodSignature( vertex );

      LOG.debug( "{} - opening property: {}, creating method: {}, group: {}, prior: {}", depth, vertex.getLhs(), methodSignature, depth, prior != null );

      int outDegree = graph.outDegreeOf( vertex );
      boolean hasTerminalPath = Graphs.successorListOf( graph, vertex ).contains( END );
      //boolean isTerminal = outDegree == 1 && hasTerminalPath;

      if( hasTerminalPath && isFactory )
        {
        blockBuilder[ 0 ] = blockBuilder[ 0 ]
          .startBlock( methodSignature )
          .last()
          .addMethod( endMethod )
          .last( type );
        }
      else if( hasTerminalPath )
        {
        blockBuilder[ 0 ] = blockBuilder[ 0 ]
          .startBlock( methodSignature )
          .last()
          .addMethod( endMethod )
          .last();
        }
      else
        {
        blockBuilder[ 0 ] = blockBuilder[ 0 ]
          .startBlock( methodSignature ).last();
        }
      }

    @Override
    public void vertexFinished( VertexTraversalEvent<StringClassPrefix> event )
      {
      StringClassPrefix vertex = event.getVertex();

      if( vertex == BEGIN || vertex == END )
        return;

      LOG.debug( "{} - closing property: {}", (int) shortestPaths.shortestDistance( BEGIN, vertex ), vertex.getLhs() );

      blockBuilder[ 0 ] = (BlockBuilder.Start<?>) blockBuilder[ 0 ]
        .endBlock();
      }
    };

    DepthFirstIterator<StringClassPrefix, Integer> iterator = new DepthFirstIterator<StringClassPrefix, Integer>( graph, BEGIN );

    iterator.addTraversalListener( listener );

    while( iterator.hasNext() )
      iterator.next();

    return blockBuilder[ 0 ];
    }

  private String createMethodSignature( String methodName, final boolean includeParameterNames, final DirectedGraph<StringClassPrefix, Integer> graph )
    {
    DepthFirstIterator<StringClassPrefix, Integer> iterator = new DepthFirstIterator<StringClassPrefix, Integer>( graph, BEGIN );

    String args = Joiner
      .on( "," )
      .skipNulls()
      .join(
        Iterators.transform( iterator, new Function<StringClassPrefix, Object>()
        {
        boolean seenArrays = false;

        @Nullable
        @Override
        public Object apply( @Nullable StringClassPrefix prefix )
          {
          if( prefix.getPair() == null ) // is BEGIN or END
            return null;

          // allow varargs if the final parameter is an array
          boolean isLast = Graphs.successorListOf( graph, prefix ).contains( END );

          try
            {
            return createMethodArg( prefix, includeParameterNames, isLast && !seenArrays );
            }
          finally
            {
            seenArrays |= prefix.getRhs().isArray();
            }
          }
        } )
      );

    return methodName + "(" + args + ")";
    }

  private String createMethodSignature( StringClassPrefix pair )
    {
    return pair.getLhs() + "(" + createMethodArg( pair, true, true ) + ")";
    }

  private String createMethodArg( StringClassPrefix pair, boolean includeParameterName, boolean varArgsAllowed )
    {
    String property = pair.getLhs();
    Class parameterType = pair.getRhs();

    final boolean isVarArg = ( (Constructor) pair.getPayload( "constructor" ) ).isVarArgs();
    final String typeName;

    if( !parameterType.isArray() )
      {
      typeName = parameterType.getName();
      }
    else if( isEnableVarArgs() && isVarArg && varArgsAllowed )
      {
      typeName = parameterType.getComponentType().getName() + "...";
      }
    else
      {
      typeName = parameterType.getComponentType().getName() + "[]";
      }

    return includeParameterName ? typeName + " " + property : typeName;
    }
  }
