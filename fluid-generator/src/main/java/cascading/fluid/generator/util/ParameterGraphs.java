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

package cascading.fluid.generator.util;

import com.google.common.base.Joiner;
import org.jgrapht.DirectedGraph;
import org.jgrapht.EdgeFactory;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.StringEdgeNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.ConstructorProperties;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class ParameterGraphs
  {
  private static final Logger LOG = LoggerFactory.getLogger( ParameterGraphs.class );

  public static final StringClassPrefix BEGIN = new StringClassPrefix( "BEGIN" );
  public static final StringClassPrefix END = new StringClassPrefix( "END" );

  public static DirectedGraph<StringClassPrefix, Integer> createParameterGraph( Set<Constructor> constructors, boolean trackPath, Class... startsWithExclusive )
    {
    Set<Class> after = new HashSet<Class>( Arrays.asList( startsWithExclusive ) );

    DirectedGraph<StringClassPrefix, Integer> graph = newGraph();

    graph.addVertex( BEGIN );
    graph.addVertex( END );

    Set<String> foundConstructors = new HashSet<String>();

    for( Constructor constructor : constructors )
      {
      ConstructorProperties annotation = (ConstructorProperties) constructor.getAnnotation( ConstructorProperties.class );

      String ctor = Joiner.on( "," ).join( annotation.value() );

      if( foundConstructors.contains( ctor ) )
        continue;

      foundConstructors.add( ctor );

      LOG.debug( "adding ctor: {}", ctor );

      StringClassPrefix lastPair = BEGIN;

      String[] propertyArray = annotation.value();
      Class[] typeArray = constructor.getParameterTypes();

      if( propertyArray.length != typeArray.length )
        throw new IllegalStateException( "parameter and type mismatch: params: " +
          Arrays.toString( propertyArray ) + ", types: " + Arrays.toString( typeArray ) );

      boolean found = after.isEmpty();

      for( int i = 0; i < annotation.value().length; i++ )
        {
        String property = propertyArray[ i ];
        Class parameterType = typeArray[ i ];

        if( i == 0 && !found && after.contains( parameterType ) )
          {
          found = true;
          continue; // skip this one
          }

        if( !found ) // wasn't found prior
          break;

        String hash = trackPath ? lastPair.getHash() : null;
        StringClassPrefix pair = new StringClassPrefix( hash, property, parameterType );

        pair.addPayload( "constructor", constructor );

        graph.addVertex( pair );

        graph.addEdge( lastPair, pair );

        lastPair = pair;
        }

      graph.addEdge( lastPair, END );
      }

    return graph;
    }

  private static <V> SimpleDirectedGraph<V, Integer> newGraph()
    {
    return new SimpleDirectedGraph<V, Integer>( new EdgeFactory<V, Integer>()
    {
    int count = 0;

    @Override
    public Integer createEdge( V v, V v2 )
      {
      return count++;
      }
    } );
    }

  public static void writeDOT( String name, DirectedGraph<StringClassPrefix, Integer> graph )
    {
    try
      {
      new File( "build/dot" ).mkdirs();

      new DOTExporter<StringClassPrefix, Integer>(
        new IntegerNameProvider<StringClassPrefix>(),
        new VertexNameProvider<StringClassPrefix>()
        {
        @Override
        public String getVertexName( StringClassPrefix prefix )
          {
          return prefix.print();
          }
        },
        new StringEdgeNameProvider<Integer>()
      ).export( new FileWriter( "build/dot/" + name + "-graph.dot" ), graph );
      }
    catch( IOException exception )
      {
      exception.printStackTrace();
      }
    }
  }
