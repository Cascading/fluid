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

package cascading.fluid.generator.util;

import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Reflection
  {
  private static final Logger LOG = LoggerFactory.getLogger( Reflection.class );

  public static Multimap<Constructor, Pair<String, Class>> createMap( Set<Constructor> constructors )
    {
    Multimap<Constructor, Pair<String, Class>> map = ArrayListMultimap.create();

    Set<String> foundConstructors = new HashSet<String>();

    for( Constructor constructor : constructors )
      {
      ConstructorProperties annotation = (ConstructorProperties) constructor.getAnnotation( ConstructorProperties.class );

      String ctor = Joiner.on( "," ).join( annotation.value() );

      if( foundConstructors.contains( ctor ) || foundConstructors.size() == 1 )
        continue;

      foundConstructors.add( ctor );

      LOG.info( "adding ctor: {}", ctor );

      for( int i = 0; i < annotation.value().length; i++ )
        {
        String property = annotation.value()[ i ];
        Class parameterType = constructor.getParameterTypes()[ i ];

        Pair<String, Class> pair = new Pair<String, Class>( property, parameterType );

        map.put( constructor, pair );
        }
      }

    return map;
    }
  }
