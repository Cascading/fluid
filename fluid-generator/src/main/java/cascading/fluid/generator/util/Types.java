/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

import cascading.pipe.Pipe;
import com.google.common.base.Predicate;
import org.reflections.ReflectionUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.beans.ConstructorProperties;
import java.lang.reflect.Constructor;
import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.util.*;

import static com.google.common.base.Predicates.and;
import static org.reflections.ReflectionUtils.getConstructors;

/**
 *
 */
public class Types
  {
  private static final Logger LOG = LoggerFactory.getLogger( Types.class );

  public static final Predicate<Member> PUBLIC = ReflectionUtils.withModifier( Modifier.PUBLIC );
  public static final Predicate<Constructor> CONSTRUCTOR_PROPERTIES = ReflectionUtils.withAnnotation( ConstructorProperties.class );

  public static <T> Map<Class<? extends T>, Set<Constructor>> getAllInstantiableSubTypes( Reflections reflections, Class<T> type, boolean allConstructors )
    {
    Map<Class<? extends T>, Set<Constructor>> types = new TreeMap<Class<? extends T>, Set<Constructor>>( new Comparator<Class<? extends T>>()
    {
    @Override
    public int compare( Class<? extends T> lhs, Class<? extends T> rhs )
      {
      return lhs.getName().compareTo( rhs.getName() );
      }
    } );

    Set<Class<? extends T>> subTypes = reflections.getSubTypesOf( type );

    LOG.debug( "for type: {}, found {} sub-types", type.getName(), subTypes.size() );

    for( Class<? extends T> subType : subTypes )
      {
      Set<Constructor> constructors;

      if( allConstructors )
        constructors = getInstantiableConstructors( subType );
      else
        constructors = getConstructorsWithNoPipes( subType );

      if( constructors.isEmpty() )
        continue;

      LOG.debug( "found sub-type: {}, with {} ctors", subType.getName(), constructors.size() );

      types.put( subType, constructors );
      }

    return types;
    }

  public static <T> Set<Constructor> getConstructorsWithNoPipes( Class<? extends T> type )
    {
    return getInstantiableConstructors( type, new Predicate<Constructor>()
    {
    private Class<?> pipeType = Reflection.loadClass( Pipe.class.getName() );

    @Override
    public boolean apply( @Nullable Constructor constructor )
      {
      Class[] parameterTypes = constructor.getParameterTypes();

      int count = 0;
      for( Class parameterType : parameterTypes )
        {
        if( parameterType.isArray() )
          parameterType = parameterType.getComponentType();

        if( pipeType.isAssignableFrom( parameterType ) )
          count++;
        }

      return count == 0;
      }
    } );
    }

  public static <T> Set<Constructor> getConstructorsWithMultiplePipes( Class<? extends T> type )
    {
    return getInstantiableConstructors( type, new Predicate<Constructor>()
    {
    private Class<?> pipeType = Reflection.loadClass( Pipe.class.getName() );

    @Override
    public boolean apply( @Nullable Constructor constructor )
      {
      Class[] parameterTypes = constructor.getParameterTypes();

      int count = 0;
      for( Class parameterType : parameterTypes )
        {
        if( parameterType.isArray() && pipeType.isAssignableFrom( parameterType.getComponentType() ) )
          return true;

        if( pipeType.isAssignableFrom( parameterType ) )
          count++;
        }

      return count > 1;
      }
    } );
    }

  public static <T> Set<Constructor> getInstantiableConstructors( Class<? extends T> type, Predicate<Constructor>... predicates )
    {
    if( !Modifier.isPublic( type.getModifiers() ) )
      return Collections.emptySet();

    if( Modifier.isAbstract( type.getModifiers() ) )
      return Collections.emptySet();

    if( type.getAnnotation( Deprecated.class ) != null )
      return Collections.emptySet();

    Predicate<Constructor> predicate = and(
      PUBLIC,
      CONSTRUCTOR_PROPERTIES
    );

    if( predicates.length != 0 )
      predicate = and( predicate, and( predicates ) );

    Set<Constructor> constructors = getConstructors( type, predicate );

    if( constructors.isEmpty() )
      return Collections.emptySet();

    return constructors;
    }
  }
