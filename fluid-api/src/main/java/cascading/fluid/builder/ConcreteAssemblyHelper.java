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

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import cascading.fluid.factory.Context;
import cascading.fluid.factory.PipeFactory;
import cascading.fluid.factory.Reflection;
import cascading.fluid.internal.assembly.Assembly.AssemblyHelper;
import cascading.fluid.internal.assembly.Branch.BranchHelper;
import cascading.fluid.internal.assembly.Group.GroupHelper;
import cascading.pipe.Checkpoint;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import com.google.common.base.Function;

/**
 *
 */
public abstract class ConcreteAssemblyHelper implements AssemblyHelper
  {
  private AssemblyMethodHandler methodHandler;

  Context context = new Context();

  public ConcreteAssemblyHelper()
    {
    }

  public ConcreteAssemblyHelper( AssemblyMethodHandler methodHandler )
    {
    setMethodHandler( methodHandler );
    }

  public void setMethodHandler( AssemblyMethodHandler methodHandler )
    {
    this.methodHandler = methodHandler;

    methodHandler.addMethod( "completeBranch", new CompleteBranchFunction() );

    // these return immediately so don't need a lazy factory
    // and bypassing the factory allows for arguments to be in a non-standard order (name, pipe) vs (pipe, arg)
    methodHandler.addMethod( "pipe", new PipeFunction() );
    methodHandler.addMethod( "checkpoint", new CheckpointFunction() );
    }

  @Override
  public void startBranch( String name, AtomicReference<BranchHelper> _helper1 )
    {
    Pipe head = new Pipe( name );

    handleStart( null, head, _helper1, BranchHelper.class );
    }

  @Override
  public Pipe[] completeAssembly()
    {
    Pipe[] tails = new Pipe[ context.branchTails.size() ];

    int count = 0;

    for( Pipe pipe : context.branchTails.values() )
      tails[ count++ ] = pipe;

    return tails;
    }

  @Override
  public void continueBranch( String name, AtomicReference<BranchHelper> _helper1 )
    {
    if( name == null )
      throw new IllegalArgumentException( "name may not be null" );

    Pipe pipe = context.branchTails.get( name );

    if( pipe == null )
      throw new IllegalArgumentException( "given pipe name not found: " + name );

    handleStart( null, pipe, _helper1, BranchHelper.class );
    }

  @Override
  public void continueBranch( String name, CoGroup coGroup, AtomicReference<GroupHelper> _helper1 )
    {
    handleStart( name, coGroup, _helper1, GroupHelper.class );
    }

  @Override
  public void continueBranch( String name, GroupBy groupby, AtomicReference<GroupHelper> _helper1 )
    {
    handleStart( name, groupby, _helper1, GroupHelper.class );
    }

  @Override
  public void continueBranch( String name, Pipe pipe, AtomicReference<BranchHelper> _helper1 )
    {
    handleStart( name, pipe, _helper1, BranchHelper.class );
    }

  @Override
  public void continueBranch( CoGroup coGroup, AtomicReference<GroupHelper> _helper1 )
    {
    handleStart( null, coGroup, _helper1, GroupHelper.class );
    }

  @Override
  public void continueBranch( GroupBy groupBy, AtomicReference<GroupHelper> _helper1 )
    {
    handleStart( null, groupBy, _helper1, GroupHelper.class );
    }

  @Override
  public void continueBranch( Pipe pipe, AtomicReference<BranchHelper> _helper1 )
    {
    handleStart( null, pipe, _helper1, BranchHelper.class );
    }

  protected <T> void handleStart( String newName, Pipe pipe, AtomicReference<T> _helper1, Class<T> interfaceType )
    {
    Pipe[] previous = pipe.getPrevious();

    for( Pipe prior : previous )
      context.branchTails.remove( prior.getName() );

    if( newName != null )
      pipe = new Pipe( newName, pipe );

    String name = pipe.getName();
    context.branchTails.put( name, pipe );
    context.currentBranch = name;

    T helper = Reflection.create( interfaceType, methodHandler, PipeFactory.class );

    ( (PipeFactory) helper ).setContext( context );

    _helper1.set( helper );
    }

  private class CompleteBranchFunction implements Function<Object[], Object>
    {
    @Nullable
    @Override
    public Object apply( @Nullable Object[] input )
      {
      return context.branchTails.get( context.currentBranch );
      }
    }

  private class PipeFunction implements Function<Object[], Object>
    {
    @Nullable
    @Override
    public Object apply( @Nullable Object[] input )
      {
      Pipe pipe = context.branchTails.get( context.currentBranch );

      if( input == null || input.length == 0 )
        throw new IllegalArgumentException( "pipe name is required" );

      Pipe result = new Pipe( (String) input[ 0 ], pipe );

      context.branchTails.remove( context.currentBranch );
      context.currentBranch = result.getName();
      context.branchTails.put( result.getName(), result );

      return result;
      }
    }

  private class CheckpointFunction implements Function<Object[], Object>
    {
    @Nullable
    @Override
    public Object apply( @Nullable Object[] input )
      {
      Pipe pipe = context.branchTails.get( context.currentBranch );

      Pipe result;

      if( input == null || input.length == 0 )
        result = new Checkpoint( pipe );
      else
        result = new Checkpoint( (String) input[ 0 ], pipe );

      context.branchTails.remove( context.currentBranch );
      context.currentBranch = result.getName();
      context.branchTails.put( result.getName(), result );

      return result;
      }
    }
  }
