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

package cascading.fluid.builder;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import cascading.fluid.factory.Factory;
import cascading.fluid.factory.MethodMeta;
import cascading.fluid.factory.PipeFactory;
import cascading.fluid.factory.Reflection;
import com.google.common.base.Function;
import javassist.util.proxy.MethodHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ReflectionMethodHandler implements MethodHandler
  {
  private static final Logger LOG = LoggerFactory.getLogger( OperationMethodHandler.class );

  Map<String, Function<Object[], Object>> methods = new HashMap<String, Function<Object[], Object>>();

  public ReflectionMethodHandler()
    {
    }

  public void addMethod( String methodName, Function<Object[], Object> function )
    {
    methods.put( methodName, function );
    }

  @Override
  public Object invoke( Object self, Method thisMethod, Method proceed, Object[] args ) throws Throwable
    {
    logMethodInfo( self, thisMethod, args );

    String methodName = thisMethod.getName();

    if( methods.containsKey( methodName ) )
      return methods.get( methodName ).apply( args );

    return handleFactory( self, thisMethod, args );
    }

  private Object handleFactory( Object self, Method thisMethod, Object[] args )
    {
    MethodMeta annotation = thisMethod.getAnnotation( MethodMeta.class );

    Class<? extends Factory> factoryType = null;
    Class createsType = null;
    String factoryMethod = null;
    boolean createOnNext = false;
    String trace = null;

    if( annotation != null )
      {
      factoryType = annotation.factory();
      createsType = annotation.creates();
      factoryMethod = annotation.method();
      createOnNext = annotation.createOnNext();
      trace = Reflection.captureDebugTrace( self.getClass(), thisMethod, factoryMethod );
      }

    if( self instanceof Factory )
      {
      Factory factory = (Factory) self;

      if( annotation == null && args.length == 0 ) // assumes we are at the end
        return factory.create();

      if( factory.isCreateOnNext() )
        factory.create(); // stores it internally
      else if( factoryType == null ) // keep forwarding type till new factory is provided
        factoryType = (Class<? extends Factory>) factory.getClass().getSuperclass();
      }

    List<Factory> children = new ArrayList<Factory>();
    List<Class> types = new ArrayList<Class>();
    List<Object> arguments = new ArrayList<Object>();

    for( int i = 0; i < args.length; i++ )
      {
      Object arg = args[ i ];

      if( !( arg instanceof AtomicReference ) )
        {
        types.add( thisMethod.getParameterTypes()[ i ] );
        arguments.add( arg );
        continue;
        }

      AtomicReference reference = (AtomicReference) arg;
      Type parameterizedType = getParameterizedType( thisMethod, i );

      LOG.debug( "reference = {}, with param = ", reference, parameterizedType );

      Object resultHelper = createHelperFromMethod( parameterizedType, this, factoryType );

      if( resultHelper instanceof Factory ) // chain factories
        {
        Factory helper = (Factory) resultHelper;

        helper.setCreatesType( createsType );
        helper.setTrace( trace );
        helper.setCreateOnNext( createOnNext );

        if( self instanceof Factory )
          helper.addPrior( (Factory) self ); // forward prior arguments

        children.add( helper );
        }

      reference.set( resultHelper );
      }

    for( Factory child : children )
      {
      child.addTypes( types );
      child.addArguments( arguments );
      }

    if( !children.isEmpty() )
      return null;

    Factory factory;

    if( annotation != null || !( self instanceof Factory ) )
      {
      if( factoryType == null )
        return null;

      factory = (Factory) createHelperFromMethod( null, this, factoryType );

      if( factory instanceof PipeFactory && self instanceof PipeFactory )
        ( (PipeFactory) factory ).setContext( ( (PipeFactory) self ).getContext() );

      factory.setCreatesType( createsType );
      factory.setTrace( trace );
      factory.setCreateOnNext( createOnNext );
      }
    else
      {
      factory = (Factory) self;
      }

    factory.addTypes( types );
    factory.addArguments( arguments );

    return factory.create();
    }

  private Object createHelperFromMethod( Type resultType, ReflectionMethodHandler methodHandler, Class<? extends Factory> superType )
    {
    return Reflection.create( (Class<Object>) resultType, methodHandler, superType );
    }

  private Type getParameterizedType( Method thisMethod, int pos )
    {
    Type type = thisMethod.getGenericParameterTypes()[ pos ];

    return ( (ParameterizedType) type ).getActualTypeArguments()[ 0 ];
    }

  private void logMethodInfo( Object self, Method thisMethod, Object[] args )
    {
    LOG.debug( "type = {}, method = {}", self.getClass().getName(), thisMethod );

    for( Type type : thisMethod.getGenericParameterTypes() )
      {
      LOG.debug( "type = {}", type );

      if( type instanceof ParameterizedType )
        {
        for( Type param : ( (ParameterizedType) type ).getActualTypeArguments() )
          LOG.debug( "paramType = {}", param );
        }
      }

    for( Object arg : args )
      LOG.debug( "arg = {}, isNull: {}", arg, ( arg == null ) );
    }
  }
