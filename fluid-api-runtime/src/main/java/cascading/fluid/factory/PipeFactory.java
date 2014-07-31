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

import java.util.LinkedList;
import java.util.List;

import cascading.pipe.Pipe;

/**
 *
 */
public class PipeFactory extends Factory
  {
  private Context context;
  private Pipe result;

  public PipeFactory()
    {
    }

  public PipeFactory( Object[] arguments, int beginIndex, int endIndex )
    {
    addArguments( arguments, beginIndex, endIndex );
    }

  public PipeFactory( List<Object> arguments )
    {
    this.arguments.addAll( arguments );
    }

  @Override
  public void addPrior( Factory prior )
    {
    super.addPrior( prior );

    if( prior instanceof PipeFactory )
      setContext( ( (PipeFactory) prior ).getContext() );
    }

  public Context getContext()
    {
    return context;
    }

  public void setContext( Context context )
    {
    this.context = context;
    }

  public Object create()
    {
    if( createsType == null )
      return result;

    logInfo( "creating: {}", createsType.getName() );

    Pipe pipe = context.branchTails.get( context.currentBranch );

    LinkedList<Class> newTypes = new LinkedList<Class>( types );
    LinkedList<Object> newArgs = new LinkedList<Object>( arguments );

    if( pipe != null )
      {
      newTypes.addFirst( pipe.getClass() );
      newArgs.addFirst( pipe );
      }
    else
      {
      newTypes.add( String.class );
      newArgs.add( context.currentBranch );
      }

    result = (Pipe) Reflection.createWith( createsType, newTypes, newArgs );

    createsType = null;
    context.currentBranch = result.getName();
    context.branchTails.put( context.currentBranch, result );
    types.clear();
    arguments.clear();

    return result;
    }
  }
