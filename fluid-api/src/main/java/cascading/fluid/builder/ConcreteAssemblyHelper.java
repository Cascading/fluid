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

package cascading.fluid.builder;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import cascading.fluid.api.assembly.Assembly.AssemblyHelper;
import cascading.fluid.api.assembly.Branch.BranchHelper;
import cascading.fluid.factory.PipeFactory;
import cascading.pipe.Pipe;
import com.google.common.base.Function;

/**
 *
 */
public class ConcreteAssemblyHelper implements AssemblyHelper
  {
  private AssemblyMethodHandler methodHandler;

  Context context = new Context();

  public ConcreteAssemblyHelper( AssemblyMethodHandler methodHandler )
    {
    this.methodHandler = methodHandler;

    methodHandler.addMethod( "completeBranch", new CompleteBranchFunction() );
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
  public void startBranch( String name, AtomicReference<BranchHelper> _helper1 )
    {
    Pipe head = new Pipe( name );

    context.branchTails.put( name, head );
    context.currentBranch = name;

    BranchHelper branchHelper = Reflection.create( BranchHelper.class, methodHandler, PipeFactory.class );

    ( (PipeFactory) branchHelper ).setContext( context );

    _helper1.set( branchHelper );
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
  }
