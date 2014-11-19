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

package cascading.fluid.factory;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Factory
  {
  protected static final Logger LOG = LoggerFactory.getLogger( Factory.class );

  Class<?> createsType = null;
  List<Class> types = new ArrayList<Class>();
  List<Object> arguments = new ArrayList<Object>();
  String trace = null;
  boolean createOnNext;

  public void addPrior( Factory prior )
    {
    if( createsType != null )
      return;

    createsType = prior.createsType;
    trace = prior.trace;
    types.addAll( prior.types );
    arguments.addAll( prior.arguments );
    }

  public void setCreatesType( Class<?> createsType )
    {
    this.createsType = createsType;
    }

  public void setCreateOnNext( boolean createOnNext )
    {
    this.createOnNext = createOnNext;
    }

  public void setTrace( String trace )
    {
    this.trace = trace;
    }

  public boolean isCreateOnNext()
    {
    return createOnNext;
    }

  public void addTypes( List<Class> types )
    {
    this.types.addAll( types );
    }

  public void addArguments( List<Object> arguments )
    {
    this.arguments.addAll( arguments );
    }

  public void addTypes( Class<?>[] parameterTypes, int beginIndex, int endIndex )
    {
    for( int i = beginIndex; i <= endIndex; i++ )
      this.types.add( parameterTypes[ i ] );
    }

  public void addArguments( Object[] arguments, int beginIndex, int endIndex )
    {
    for( int i = beginIndex; i <= endIndex; i++ )
      this.arguments.add( arguments[ i ] );
    }

  public Object create()
    {
    return instantiate( types, arguments );
    }

  protected void logInfo( String message, Object... values )
    {
    LOG.debug( message, values );
    }

  protected Object instantiate( List<Class> types, List<Object> args )
    {
    logInfo( "creating: {}", createsType.getName() );

    Object result = Reflection.createWith( createsType, types, args );

    if( trace != null && !trace.isEmpty() )
      Reflection.setTraceOn( result, trace );

    return result;
    }
  }
