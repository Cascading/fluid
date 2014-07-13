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
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.reflections.ReflectionUtils.getConstructors;
import static org.reflections.ReflectionUtils.withAnnotation;

/**
 *
 */
public class Types
  {
  private static final Logger LOG = LoggerFactory.getLogger( Types.class );

  public static <T> Map<Class<? extends T>, Set<Constructor>> getAllInstantiable( Reflections reflections, Class<T> type )
    {
    Map<Class<? extends T>, Set<Constructor>> types = new HashMap<Class<? extends T>, Set<Constructor>>();
    Set<Class<? extends T>> subTypes = reflections.getSubTypesOf( type );

    LOG.info( "for type: {}, found {} sub-types", type.getName(), subTypes.size() );

    for( Class<? extends T> subType : subTypes )
      {
      if( Modifier.isAbstract( subType.getModifiers() ) )
        continue;

      if( subType.getAnnotation( Deprecated.class ) != null )
        continue;

      Set<Constructor> constructors = getConstructors( subType, withAnnotation( ConstructorProperties.class ) );

      if( constructors.isEmpty() )
        continue;

      LOG.info( "found sub-type: {}, with {} ctors", subType.getName(), constructors.size() );

      types.put( subType, constructors );
      }

    return types;
    }
  }
