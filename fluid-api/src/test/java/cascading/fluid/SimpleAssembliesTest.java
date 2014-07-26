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

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.fluid.api.assembly.Assembly.AssemblyBuilder;
import cascading.operation.Function;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.OuterJoin;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import org.junit.Test;

import static cascading.flow.FlowDef.flowDef;
import static cascading.fluid.Fluid.*;
import static cascading.pipe.Pipe.pipes;

/**
 *
 */
public class SimpleAssembliesTest
  {
  @Test
  public void testSimpleGroup() throws Exception
    {
    // @formatter:off
    Tap source = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/path" );
    Tap sink = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/otherpath", SinkMode.REPLACE );

    AssemblyBuilder.Start assembly = Fluid.assembly();

    Pipe pipe = assembly
      .startBranch( "test" )
        .each( fields( "line" ) )
          .function(
            function().RegexParser().fieldDeclaration( fields( "ip" ) ).patternString( "^[^ ]*" ).end()
          )
        .outgoing( fields( "ip" ) )

        .groupBy( fields( "ip" ) )
          .every( Fields.ALL )
            .aggregator(
              aggregator().Count( fields( "count" ) )
            )
          .outgoing( fields( "ip", "count" ) )
        .completeGroupBy()

      .completeBranch();
    // @formatter:on

    Flow flow = new LocalFlowConnector().connect( source, sink, pipe );
    }

  @Test
  public void testGroupByMerge()
    {
    Tap sourceLower = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/lower" );
    Tap sourceUpper = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/upper" );
    Tap sourceLowerOffset = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/offset" );

    Tap sink = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/result", SinkMode.REPLACE );

    // @formatter:off
    Function splitter = function()
      .RegexSplitter()
        .fieldDeclaration( fields( "num", "char" ) )
        .patternString( " " )
      .end();

    AssemblyBuilder.Start assembly = Fluid.assembly();

    Pipe pipeLower = assembly
      .startBranch( "lower" )
        .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
      .completeBranch();

    Pipe pipeUpper = assembly
      .startBranch( "upper" )
        .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
      .completeBranch();

    Pipe pipeOffset = assembly
      .startBranch( "offset" )
        .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
      .completeBranch();

    GroupBy merge = assembly
      .startGroupByMerge()
        .pipes( pipes( pipeLower, pipeUpper, pipeOffset ) )
        .groupFields( fields( "num" ) )
        .sortFields( fields( "char" ) )
      .createGroupByMerge();

    Pipe splice = assembly
      .continueBranch( merge )
        .every( fields( "char" ) )
          .aggregator(
            aggregator().First().fieldDeclaration( fields( "first" ) ).end()
          )
          .outgoing( Fields.ALL )
        .each( fields( "num", "first" ) )
          .function(
            function().Identity().fieldDeclaration( Fields.ALL ).end()
          )
        .outgoing( Fields.RESULTS )
      .completeBranch();

    FlowDef flowDef = flowDef()
      .addSource( "lower", sourceLower )
      .addSource( "upper", sourceUpper )
      .addSource( "offset", sourceLowerOffset )
      .addTailSink( splice, sink );
    // @formatter:on

    Flow flow = new LocalFlowConnector().connect( flowDef );
    }

  @Test
  public void testCoGroup() throws Exception
    {
    Tap sourceLower = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/lower" );
    Tap sourceUpper = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/upper" );

    Tap sink = new FileTap( new TextLine( new Fields( "line" ) ), "some/result", SinkMode.REPLACE );

    // @formatter:off
    // Factories for all Operations (Functions, Filters, Aggregators, and Buffers)
    Function splitter = Fluid.function()
      .RegexSplitter()
      .fieldDeclaration( fields( "num", "char" ) )
      .patternString( " " )
      .end();

    // An assembly builder chaining Pipes into complex assemblies
    AssemblyBuilder.Start assembly = Fluid.assembly();

    Pipe pipeLower = assembly
      .startBranch( "lower" )
      .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
      .completeBranch();

    Pipe pipeUpper = assembly
      .startBranch( "upper" )
      .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
      .completeBranch();

    Pipe coGroup = assembly
      .startCoGroup()
      .lhs( pipeLower ).lhsGroupFields( fields( "num" ) )
      .rhs( pipeUpper ).rhsGroupFields( fields( "num" ) )
      .declaredFields( fields( "num1", "char1", "num2", "char2" ) )
      .joiner( new OuterJoin() )
      .createCoGroup();

    assembly
      .continueBranch( "result", coGroup )
      .retain( fields( "num1", "char1" ) )
      .rename( Fields.ALL, fields( "num", "char" ) )
      .completeBranch();

    Pipe[] tails = assembly.completeAssembly();

    FlowDef flowDef = flowDef()
      .addSource( "lower", sourceLower )
      .addSource( "upper", sourceUpper )
      .addSink( "result", sink )
      .addTails( tails );
    // @formatter:on

    Flow flow = new LocalFlowConnector().connect( flowDef );
    }
  }
