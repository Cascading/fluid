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

package cascading.fluid;

import java.lang.reflect.Type;

import cascading.fluid.api.assembly.Assembly.AssemblyGenerator;
import cascading.fluid.api.assembly.Assembly.AssemblyHelper;
import cascading.fluid.api.operation.Operation.OperationBuilder;
import cascading.fluid.api.operation.Operation.OperationGenerator;
import cascading.fluid.api.operation.Operation.OperationHelper;
import cascading.fluid.builder.AssemblyMethodHandler;
import cascading.fluid.builder.ConcreteAssemblyHelper;
import cascading.fluid.builder.LocalMethodLogger;
import cascading.fluid.builder.OperationMethodHandler;
import cascading.fluid.builder.Reflection;
import cascading.tuple.Fields;

/**
 *
 */
public class Fluid
  {
  public static Fields fields( Comparable... fields )
    {
    return new Fields( fields );
    }

  public static Fields fields( Comparable field, Type type )
    {
    return new Fields( field, type );
    }

  public static cascading.fluid.api.assembly.Assembly.AssemblyBuilder.Start assembly()
    {
    AssemblyMethodHandler methodHandler = new AssemblyMethodHandler();
    AssemblyHelper helper = Reflection.create( AssemblyHelper.class, methodHandler, ConcreteAssemblyHelper.class );

    ( (ConcreteAssemblyHelper) helper ).setMethodHandler( methodHandler );

    return AssemblyGenerator.startAssembly( helper, new LocalMethodLogger() );
    }

  private static OperationBuilder.Start getOperationBuilder()
    {
    OperationHelper operationHelper = Reflection.create( OperationHelper.class, new OperationMethodHandler() );

    return OperationGenerator.build( operationHelper, new LocalMethodLogger() );
    }

  public static cascading.fluid.api.operation.Function.FunctionBuilder<Void> function()
    {
    return getOperationBuilder().function();
    }

  public static cascading.fluid.api.operation.Filter.FilterBuilder<Void> filter()
    {
    return getOperationBuilder().filter();
    }

  public static cascading.fluid.api.operation.Aggregator.AggregatorBuilder<Void> aggregator()
    {
    return getOperationBuilder().aggregator();
    }

  public static cascading.fluid.api.operation.Buffer.BufferBuilder<Void> buffer()
    {
    return getOperationBuilder().buffer();
    }
  }
