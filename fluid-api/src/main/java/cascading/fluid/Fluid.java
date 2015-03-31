/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;

import cascading.fluid.api.operation.Operation.OperationBuilder;
import cascading.fluid.api.subassembly.SubAssembly.SubAssemblyBuilder;
import cascading.fluid.builder.AssemblyMethodHandler;
import cascading.fluid.builder.ConcreteAssemblyHelper;
import cascading.fluid.builder.LocalMethodLogger;
import cascading.fluid.builder.OperationMethodHandler;
import cascading.fluid.builder.SubAssemblyMethodHandler;
import cascading.fluid.factory.Reflection;
import cascading.fluid.internal.assembly.Assembly.AssemblyBuilder;
import cascading.fluid.internal.assembly.Assembly.AssemblyGenerator;
import cascading.fluid.internal.assembly.Assembly.AssemblyHelper;
import cascading.fluid.internal.operation.Aggregator.AggregatorBuilder;
import cascading.fluid.internal.operation.Buffer.BufferBuilder;
import cascading.fluid.internal.operation.Filter.FilterBuilder;
import cascading.fluid.internal.operation.Function.FunctionBuilder;
import cascading.fluid.internal.operation.GroupAssertion.GroupAssertionBuilder;
import cascading.fluid.internal.operation.Operation.OperationGenerator;
import cascading.fluid.internal.operation.Operation.OperationHelper;
import cascading.fluid.internal.operation.ValueAssertion.ValueAssertionBuilder;
import cascading.fluid.internal.subassembly.AggregateBy.AggregateByBuilder;
import cascading.fluid.internal.subassembly.SubAssembly.SubAssemblyGenerator;
import cascading.fluid.internal.subassembly.SubAssembly.SubAssemblyHelper;
import cascading.fluid.util.Version;
import cascading.property.AppProps;
import cascading.tuple.Fields;

/**
 * The Fluid class is the starting point for constructing new Pipe Assemblies via this API.
 * </p></p>
 * To get started, a new assembly builder must be created:
 * <p></p>
 * <pre>
 *   AssemblyBuilder.Start builder = Fluid.assembly();
 * </pre>
 * <p></p>
 * Next a branch must be started:
 * <p></p>
 * <pre>
 *  Pipe pipe = builder.startBranch( "rhs" )
 *    .groupBy( Fields.ALL )
 *      .every( Fields.ALL ).aggregator( new Count() ).outgoing( Fields.ALL )
 *    .completeGroupBy()
 *     .each( Fields.ALL ).filter( new RegexFilter( "" ) )
 *       .coerce().coerceFields( Fields.fields( "foo", int.class ) ).end()
 *  .completeBranch();
 * </pre>
 * <p></p>
 * Note {@code completeBranch()} is a factory, it will return a {@link cascading.pipe.Pipe} instance. Also note
 * the assembly builder is stateful, and will keep the return Pipe as a known assembly tail.
 * <p></p>
 * {@link #fields(Comparable[])} is a convenience for {@code new Fields("...")}. Type Fields itself also has many fluent helper
 * methods, for example {@code Fluid.fields( "average").applyTypes(long.class);}
 * <p></p>
 * Calling:
 * <p></p>
 * <pre>
 *  Pipe[] tails = assembly.completeAssembly();
 * </pre>
 * Will return a {@code Pipe[]} with a single entry, the same called from the {@code completeBranch()} call
 * previously.
 * <p></p>
 * To begin a join, two or more pipes will need to be created prior to the next call:
 * </p></p>
 * <pre>
 *  Pipe lhsUpperLower = assembly
 *    .startHashJoin()
 *      .lhs( pipeLhs ).lhsJoinFields( fields( "num" ) )
 *      .rhs( upperLower ).rhsJoinFields( fields( "numUpperLower" ) )
 *      .declaredFields( fields( "numLhs", "charLhs", "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower" ) )
 *    .createHashJoin();
 * </pre>
 * <p></p>
 * This is a factory and must be added to the assembly via:
 * <p></p>
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
 * <p></p>
 * If the two given pipes ({@code pipeLhs} and {@code pipeRhs}) were previously tails
 * in the assembly, they will be no longer tails within the assembly, replaced by the result of {@code completeBranch()}.
 * <p></p>
 * Finally notice in the above code, {@code function()} is used to create a new {@link cascading.operation.Function}
 * for use in the assembly.
 * <p></p>
 * In the first example at the top, {@code new Count()} was called. This call could have
 * been replaced with {@code function().Count().end()}.
 * <p></p>
 * AggregateBy sub-classes also have factory helpers than can be used when adding a base
 * {@link cascading.pipe.assembly.AggregateBy} to the pipe assembly.
 * <p></p>
 * <pre>
 * Pipe rhs = builder
 *  .startBranch( "rhs" )
 *    .aggregateBy()
 *      .groupingFields( fields( "grouping" ) )
 *      .assemblies
 *        (
 *          Fluid.aggregateBy().AverageBy().valueField( fields( "value" ) ).averageField( fields( "average" ) ).end(),
 *          Fluid.aggregateBy().SumBy().valueField( fields( "value" ) ).sumField( fields( "sum", long.class ) ).end()
 *        )
 *    .end() // end aggregateBy builder
 *  .completeBranch();
 * </pre>
 */
public class Fluid
  {
  static
    {
    AppProps.addApplicationFramework( null, Version.getName() + ":" + Version.getVersionString() );
    }

  private Fluid()
    {
    }

  /**
   * Method fields is a convenience helper factory for creating a new {@link cascading.tuple.Fields} instance.
   * <p></p>
   * The Fields class is also fluent, so adding types to the result is as simple as calling
   * {@link Fields#applyTypes(java.lang.reflect.Type...)}.
   *
   * @param fields is a set of integer ordinals or field names.
   * @return a new Fields instance.
   * @see cascading.tuple.Fields
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
   * @see cascading.tuple.Fields
   */
  public static Fields fields( Comparable field, Type type )
    {
    return new Fields( field, type );
    }

  /**
   * Method assembly returns a new assembly builder.
   * <p></p>
   * An assembly is a collection of branches.
   * <p></p>
   * To add a new branch, call {@link cascading.fluid.api.assembly.Assembly.AssemblyBuilder.Start#startBranch(String)}.
   * <p></p>
   * To complete the current branch, call {@code completeBranch()} on the builder.
   * <p></p>
   * To complete the assembly, call  {@link cascading.fluid.api.assembly.Assembly.AssemblyBuilder.Start#completeAssembly()}.
   *
   * @return a new Assembly builder instance
   */
  public static cascading.fluid.api.assembly.Assembly.AssemblyBuilder.Start assembly()
    {
    AssemblyMethodHandler methodHandler = new AssemblyMethodHandler();
    AssemblyHelper helper = Reflection.create( AssemblyHelper.class, methodHandler, ConcreteAssemblyHelper.class );

    ( (ConcreteAssemblyHelper) helper ).setMethodHandler( methodHandler );

    AssemblyBuilder.Start<Void> builder = AssemblyGenerator.startAssembly( helper, new LocalMethodLogger() );
    return simpleProxy( cascading.fluid.api.assembly.Assembly.AssemblyBuilder.Start.class, builder );
    }

  private static OperationBuilder.Start getOperationBuilder()
    {
    OperationHelper operationHelper = Reflection.create( OperationHelper.class, new OperationMethodHandler() );
    cascading.fluid.internal.operation.Operation.OperationBuilder.Start<Void> builder
      = OperationGenerator.build( operationHelper, new LocalMethodLogger() );

    return simpleProxy( OperationBuilder.Start.class, builder );
    }

  /**
   * Method function returns a new {@link cascading.operation.Function} factory builder.
   * <p></p>
   * Unlike the assembly builder, a Function factory builder provides a simple api for constructing
   * known Function types, that should ba added after an {@code each()} builder method is called.
   * <p></p>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new Function builder instance
   * @see cascading.operation.Function
   */
  public static cascading.fluid.api.operation.Function.FunctionBuilder<Void> function()
    {
    FunctionBuilder.Start<Void> builder = getOperationBuilder().function();
    return simpleProxy( cascading.fluid.api.operation.Function.FunctionBuilder.class, builder );
    }

  /**
   * Method filter returns a new {@link cascading.operation.Filter} factory builder.
   * <p></p>
   * Unlike the assembly builder, a Filter factory builder provides a simple api for constructing
   * known Filter types, that should ba added after an {@code each()} builder method is called.
   * <p></p>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new Filter builder instance
   * @see cascading.operation.Filter
   */
  public static cascading.fluid.api.operation.Filter.FilterBuilder<Void> filter()
    {
    FilterBuilder.Start<Void> builder = getOperationBuilder().filter();
    return simpleProxy( cascading.fluid.api.operation.Filter.FilterBuilder.class, builder );
    }

  /**
   * Method aggregator returns a new {@link cascading.operation.Aggregator} factory builder.
   * <p></p>
   * Unlike the assembly builder, a Aggregator factory builder provides a simple api for constructing
   * known Aggregator types, that should ba added after an {@code every()} builder method is called.
   * <p></p>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new Aggregator builder instance
   * @see cascading.operation.Aggregator
   */
  public static cascading.fluid.api.operation.Aggregator.AggregatorBuilder<Void> aggregator()
    {
    AggregatorBuilder.Start<Void> builder = getOperationBuilder().aggregator();
    return simpleProxy( cascading.fluid.api.operation.Aggregator.AggregatorBuilder.class, builder );
    }

  /**
   * Method buffer returns a new {@link cascading.operation.Aggregator} factory builder.
   * <p></p>
   * Unlike the assembly builder, a Buffer factory builder provides a simple api for constructing
   * known Buffer types, that should ba added after an {@code every()} builder method is called.
   * <p></p>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new Buffer builder instance
   * @see cascading.operation.Buffer
   */
  public static cascading.fluid.api.operation.Buffer.BufferBuilder<Void> buffer()
    {
    BufferBuilder.Start<Void> builder = getOperationBuilder().buffer();
    return simpleProxy( cascading.fluid.api.operation.Buffer.BufferBuilder.class, builder );
    }

  /**
   * Method valueAssertion returns a new {@link cascading.operation.ValueAssertion} factory builder.
   * <p></p>
   * Unlike the assembly builder, a ValueAssertion factory builder provides a simple api for constructing
   * known ValueAssertion types, that should ba added after an {@code each()} builder method is called.
   * <p></p>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new ValueAssertion builder instance
   * @see cascading.operation.ValueAssertion
   */
  public static cascading.fluid.api.operation.ValueAssertion.ValueAssertionBuilder<Void> valueAssertion()
    {
    ValueAssertionBuilder.Start<Void> builder = getOperationBuilder().valueAssertion();
    return simpleProxy( cascading.fluid.api.operation.ValueAssertion.ValueAssertionBuilder.class, builder );
    }

  /**
   * Method groupAssertion returns a new {@link cascading.operation.GroupAssertion} factory builder.
   * <p></p>
   * Unlike the assembly builder, a GroupAssertion factory builder provides a simple api for constructing
   * known GroupAssertion types, that should ba added after an {@code every()} builder method is called.
   * <p></p>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new GroupAssertion builder instance
   * @see cascading.operation.GroupAssertion
   */
  public static cascading.fluid.api.operation.GroupAssertion.GroupAssertionBuilder<Void> groupAssertion()
    {
    GroupAssertionBuilder.Start<Void> builder = getOperationBuilder().groupAssertion();
    return simpleProxy( cascading.fluid.api.operation.GroupAssertion.GroupAssertionBuilder.class, builder );
    }

  private static SubAssemblyBuilder.Start getSubAssemblyBuilder()
    {
    SubAssemblyHelper subassemblyHelper = Reflection.create( SubAssemblyHelper.class, new SubAssemblyMethodHandler() );
    cascading.fluid.internal.subassembly.SubAssembly.SubAssemblyBuilder.Start<Void> builder
      = SubAssemblyGenerator.build( subassemblyHelper, new LocalMethodLogger() );

    return simpleProxy( SubAssemblyBuilder.Start.class, builder );
    }

  /**
   * Method aggregateBy returns a new {@link cascading.pipe.assembly.AggregateBy} factory builder.
   * <p></p>
   * Unlike the assembly builder, an AggregateBy factory builder provides a simple api for constructing
   * known AggregateBy sub-types that should be passed to a parent {@link cascading.pipe.assembly.AggregateBy} for
   * concurrent aggregation of multiple values.
   * <p></p>
   * Factory builders retain no internal state, and can be shared and re-used across assembly builders.
   *
   * @return a new AggregateBy builder instance
   * @see cascading.pipe.assembly.AggregateBy
   */
  public static cascading.fluid.api.subassembly.AggregateBy.AggregateByBuilder<Void> aggregateBy()
    {
    AggregateByBuilder.Start<Void> builder = getSubAssemblyBuilder().aggregateBy();
    return simpleProxy( cascading.fluid.api.subassembly.AggregateBy.AggregateByBuilder.class, builder );
    }

  @SuppressWarnings("unchecked")
  private static <T> T simpleProxy( final Class<?> proxyInterface, final Object target )
    {
    return (T) Proxy.newProxyInstance( Thread.currentThread().getContextClassLoader(), new Class[]{
      proxyInterface}, new InvocationHandler()
    {
    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
      {
      return method.invoke( target, args );
      }
    } );
    }
  }
