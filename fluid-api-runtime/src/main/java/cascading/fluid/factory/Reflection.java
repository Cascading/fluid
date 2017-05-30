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

package cascading.fluid.factory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cascading.fluid.FluidException;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.reflections.ReflectionUtils.getConstructors;
import static org.reflections.ReflectionUtils.withParametersAssignableTo;

/**
 *
 */
public class Reflection
  {
  private static final Logger LOG = LoggerFactory.getLogger( Reflection.class );

  public static <T> T create( Class<T> interfaceType, MethodHandler methodHandler )
    {
    return create( interfaceType, methodHandler, null );
    }

  public static <T> T create( Class<T> interfaceType, MethodHandler methodHandler, Class superType )
    {
    Class[] paramTypes = {};
    Object[] args = {};

    return create( interfaceType, methodHandler, superType, paramTypes, args );
    }

  static <T> T create( Class<T> interfaceType, MethodHandler methodHandler, Class superType, Class[] paramTypes, Object[] args )
    {
    ProxyFactory proxyFactory = new ProxyFactory();

    if( superType != null )
      proxyFactory.setSuperclass( superType );

    if( interfaceType != null )
      proxyFactory.setInterfaces( new Class[]{interfaceType} );

    proxyFactory.setFilter( new HelperMethodFilter( superType ) );

    try
      {
      return (T) proxyFactory.create( paramTypes, args, methodHandler );
      }
    catch( NoSuchMethodException exception )
      {
      throw new FluidException( "failed creating helper", exception );
      }
    catch( InstantiationException exception )
      {
      throw new FluidException( "failed creating helper", exception );
      }
    catch( IllegalAccessException exception )
      {
      throw new FluidException( "failed creating helper", exception );
      }
    catch( InvocationTargetException exception )
      {
      throw new FluidException( "failed creating helper", exception );
      }
    }

  public static <T> T createWith( Class<T> type, List<Class> types, List<Object> arguments )
    {
    Class[] typeArray = types.toArray( new Class[ types.size() ] );

    Set<Constructor> constructors = getConstructors( type, withParametersAssignableTo( typeArray ) );

    if( constructors.size() != 1 )
      throw new FluidException( "could not find constructor for: " + type + ", with: " + types );

    Constructor constructor = constructors.iterator().next();

    Object[] argsArray = arguments.toArray( new Object[ arguments.size() ] );

    return newInstance( constructor, argsArray );
    }

  private static <T> T newInstance( Constructor constructor, Object[] argsArray )
    {
    try
      {
      return (T) constructor.newInstance( argsArray );
      }
    catch( InstantiationException exception )
      {
      throw new FluidException( "unable to create type for: " + constructor, exception );
      }
    catch( IllegalAccessException exception )
      {
      throw new FluidException( "unable to create type for: " + constructor, exception );
      }
    catch( InvocationTargetException exception )
      {
      throw new FluidException( "unable to create type for: " + constructor, exception.getTargetException() );
      }
    }

  public static String captureDebugTrace( Class type, Method thisMethod, String factoryMethod )
    {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    Package packageName = type.getPackage();
    String typeName = type.getName();
    String methodName = thisMethod.getName();

    boolean skip = true;

    for( int i = 0; i < stackTrace.length; i++ )
      {
      StackTraceElement stackTraceElement = stackTrace[ i ];
      String className = stackTraceElement.getClassName();

      if( skip )
        {
        skip = !className.equals( typeName );
        continue;
        }

      if( packageName != null && stackTraceElement.getClassName().equals( typeName ) )
        continue;

      if( methodName != null && !stackTraceElement.getMethodName().equals( methodName ) )
        continue;

      return String.format( "%s @ %s", factoryMethod, stackTrace[ i + 1 ] );
      }

    return null;
    }

  static void setTraceOn( Object result, String trace )
    {
    setInstanceFieldIfExists( result, "trace", trace );
    }

  public static <R> void setInstanceFieldIfExistsSafe( Object target, String fieldName, R value )
    {
    try
      {
      setInstanceFieldIfExists( target, fieldName, value );
      }
    catch( Exception exception )
      {
      // do nothing
      }
    }

  public static <R> R returnInstanceFieldIfExistsSafe( Object target, String fieldName )
    {
    try
      {
      return returnInstanceFieldIfExists( target, fieldName );
      }
    catch( Exception exception )
      {
      // do nothing
      return null;
      }
    }

  public static <R> R returnInstanceFieldIfExists( Object target, String fieldName )
    {
    try
      {
      Class<?> type = target.getClass();
      Field field = getDeclaredField( fieldName, type );

      if ( field == null )
        return null;

      field.setAccessible( true );

      return (R) field.get( target );
      }
    catch( Exception exception )
      {
      throw new FluidException( "unable to get instance field: " + target.getClass().getName() + "." + fieldName, exception );
      }
    }

  public static <R> void setInstanceFieldIfExists( Object target, String fieldName, R value )
    {
    try
      {
      Class<?> type = target.getClass();
      Field field = getDeclaredField( fieldName, type );

      field.setAccessible( true );

      field.set( target, value );
      }
    catch( Exception exception )
      {
      throw new FluidException( "unable to set instance field: " + target.getClass().getName() + "." + fieldName, exception );
      }
    }

  private static Field getDeclaredField( String fieldName, Class<?> type )
    {
    if( type == Object.class )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "did not find {} field on {}", fieldName, type.getName() );

      return null;
      }

    try
      {
      return type.getDeclaredField( fieldName );
      }
    catch( NoSuchFieldException exception )
      {
      return getDeclaredField( fieldName, type.getSuperclass() );
      }
    }

  private static class HelperMethodFilter implements MethodFilter
    {
    Set<String> implemented = new HashSet<String>();

    {
    implemented.add( "finalize" );
    implemented.add( "toString" );
    }

    private HelperMethodFilter( Class type )
      {
      if( type == null )
        return;

      while( type != null && type != Object.class )
        {
        for( Method method : type.getDeclaredMethods() )
          implemented.add( method.getName() );

        type = type.getSuperclass();
        }
      }

    @Override
    public boolean isHandled( Method m )
      {
      return !implemented.contains( m.getName() );
      }
    }
  }
