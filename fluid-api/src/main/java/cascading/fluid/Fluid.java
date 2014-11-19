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
import cascading.fluid.factory.Reflection;
import cascading.fluid.util.Version;
import cascading.property.AppProps;
import cascading.tuple.Fields;

/**
 * The Fluid class is the starting point for constructing new Pipe Assemblies via this API.
 * <p/>
 * To get started, a new assembly builder must be created:
 * <p/>
 * <pre>
 *   AssemblyBuilder.Start builder = Fluid.assembly();
 * </pre>
 * <p/>
 * Next a branch must be started:
 * <p/>
 * <pre>
 *  Pipe pipe = builder.startBranch( "rhs" )
 *    .groupBy( Fields.ALL )
 *      .every( Fields.ALL ).aggregator( new Count() ).outgoing( Fields.ALL )
 *    .completeGroupBy()
 *     .each( Fields.ALL ).filter( new RegexFilter( "" ) )
 *       .coerce().coerceFields( fields( "foo", int.class ) ).end()
 *  .completeBranch();
 * </pre>
 * <p/>
 * Note {@code completeBranch()} is a factory, it will return a {@link cascading.pipe.Pipe} instance. Also note
 * the assembly builder is stateful, and will keep the return Pipe as a known assembly tail.
 * <p/>
 * Calling:
 * <p/>
 * <pre>
 *  Pipe[] tails = assembly.completeAssembly();
 * </pre>
 * Will return a {@code Pipe[]} with a single entry, the same called from the {@code completeBranch()} call
 * previously.
 * <p/>
 * To begin a join, two or more pipes will need to be created prior to the next call:
 * <p/>
 * <pre>
 *  Pipe lhsUpperLower = assembly
 *    .startHashJoin()
 *      .lhs( pipeLhs ).lhsJoinFields( fields( "num" ) )
 *      .rhs( upperLower ).rhsJoinFields( fields( "numUpperLower" ) )
 *      .declaredFields( fields( "numLhs", "charLhs", "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower" ) )
 *    .createHashJoin();
 * </pre>
 * <p/>
 * This is a factory and must be added to the assembly via:
 * <p/>
 * <pre>
 * lhsUpperLower = assembly
 *  .continueBranch( lhsUpperLower )
 *    .each( Fields.ALL ).function
 *      (
 *      function().Identity().fieldDeclaration( Fields.ALL ).end()
 *      )
 *    .outgoing( Fields.RESULTS )
 *  .completeBranch();
 * </pre>
 * <p/>
 * If the two given pipes ({@code pipeLhs} and {@code pipeRhs}) were previously tails
 * in the assembly, they will be no longer tails within the assembly, replaced by the result of {@code completeBranch()}.
 * <p/>
 * Finally notice in the above code, {@code function()} is used to create a new {@link cascading.operation.Function}
 * for use in the assembly.
 * <p/>
 * In the first example at the top, {@code new Count()} was called. This call could have
 * been replaced with {@code function().Count().end()}.
 */
public class Fluid
  {
  static
    {
    AppProps.addApplicationFramework( null, Version.getName() + ":" + Version.getVersionString() );
    }

  /**
   * Method fields is a convenience helper factory for creating a new {@link cascading.tuple.Fields} instance.
   * <p/>
   * The Fields class is also fluent, so adding types to the result is as simple as calling
   * {@link Fields#applyTypes(java.lang.reflect.Type...)}.
   *
   * @param fields is a set of integer ordinals or field names.
   * @return a new Fields instance.
   */
  public static Fields fields( Comparable... fields )
    {
    return new Fields( fields );
    }

  /**
   * Method fields is a convenience helper factory for creating a new {@link cascading.tuple.Fields} instance.
   *
   * @param field is a an integer ordinal or field name.
   * @param type  is the Type of the given field
   * @return a new Fields instance.
   */
  public static Fields fields( Comparable field, Type type )
    {
    return new Fields( field, type );
    }

  /**
   * Method assembly returns a new assembly builder.
   * <p/>
   * An assembly is a collection of branches.
   * <p/>
   * To add a new branch, call {@link cascading.fluid.api.assembly.Assembly.AssemblyBuilder.Start#startBranch(String)}.
   * <p/>
   * To complete the current branch, call {@code completeBranch()} on the builder.
   * <p/>
   * To complete the assembly, call  {@link cascading.fluid.api.assembly.Assembly.AssemblyBuilder.Start#completeAssembly()}.
   *
   * @return a new assembly builder instance
   */
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

  /**
   * Method function returns a new {@link cascading.operation.Function} factory builder.
   * <p/>
   * Unlike the assembly builder, a Function factory builder provides a simple api for constructing
   * known Function types, that should ba added after an {@code each()} builder method is called.
   * <p/>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new function builder instance
   */
  public static cascading.fluid.api.operation.Function.FunctionBuilder<Void> function()
    {
    return getOperationBuilder().function();
    }

  /**
   * Method filter returns a new {@link cascading.operation.Filter} factory builder.
   * <p/>
   * Unlike the assembly builder, a Filter factory builder provides a simple api for constructing
   * known Filter types, that should ba added after an {@code each()} builder method is called.
   * <p/>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new filter builder instance
   */
  public static cascading.fluid.api.operation.Filter.FilterBuilder<Void> filter()
    {
    return getOperationBuilder().filter();
    }

  /**
   * Method aggregator returns a new {@link cascading.operation.Aggregator} factory builder.
   * <p/>
   * Unlike the assembly builder, a Aggregator factory builder provides a simple api for constructing
   * known Aggregator types, that should ba added after an {@code every()} builder method is called.
   * <p/>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new aggregator builder instance
   */
  public static cascading.fluid.api.operation.Aggregator.AggregatorBuilder<Void> aggregator()
    {
    return getOperationBuilder().aggregator();
    }

  /**
   * Method buffer returns a new {@link cascading.operation.Aggregator} factory builder.
   * <p/>
   * Unlike the assembly builder, a Buffer factory builder provides a simple api for constructing
   * known Buffer types, that should ba added after an {@code every()} builder method is called.
   * <p/>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new buffer builder instance
   */
  public static cascading.fluid.api.operation.Buffer.BufferBuilder<Void> buffer()
    {
    return getOperationBuilder().buffer();
    }

  /**
   * Method valueAssertion returns a new {@link cascading.operation.ValueAssertion} factory builder.
   * <p/>
   * Unlike the assembly builder, a ValueAssertion factory builder provides a simple api for constructing
   * known ValueAssertion types, that should ba added after an {@code each()} builder method is called.
   * <p/>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new valueAssertion builder instance
   */
  public static cascading.fluid.api.operation.ValueAssertion.ValueAssertionBuilder<Void> valueAssertion()
    {
    return getOperationBuilder().valueAssertion();
    }

  /**
   * Method groupAssertion returns a new {@link cascading.operation.GroupAssertion} factory builder.
   * <p/>
   * Unlike the assembly builder, a GroupAssertion factory builder provides a simple api for constructing
   * known GroupAssertion types, that should ba added after an {@code every()} builder method is called.
   * <p/>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new groupAssertion builder instance
   */
  public static cascading.fluid.api.operation.GroupAssertion.GroupAssertionBuilder<Void> groupAssertion()
    {
    return getOperationBuilder().groupAssertion();
    }
  }
