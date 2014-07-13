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

import cascading.fluid.api.assembly.Assembly.AssemblyBuilder;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class SimpleTest
  {
  @Test
  public void testAssemblyBuilder()
    {
    AssemblyBuilder.Start builder = Fluid.assembly();

    Pipe rhs = builder.startBranch( "rhs" ).groupBy( Fields.ALL )
      .every( Fields.ALL ).aggregator( new Count() ).outgoing( Fields.ALL )
      .complete()
      .each( Fields.ALL ).filter( new RegexFilter( "" ) ).completeBranch();

    assertNotNull( rhs );
    assertTrue( rhs instanceof Each );

    Pipe lhs = builder.startBranch( "lhs" )
      .each( Fields.ALL ).function( new Identity() ).outgoing( Fields.RESULTS )
      .each( Fields.ALL ).filter( new RegexFilter( "" ) )
      .groupBy( Fields.ALL ).complete()
      .completeBranch();

    assertNotNull( lhs );
    assertTrue( lhs instanceof GroupBy );
    assertNotNull( lhs.getPrevious() );

    // startJoin( lhs, rhs ) etc etc

    Pipe[] tails = builder.completeAssembly();

    assertNotNull( tails );
    assertEquals( 2, tails.length );
    }

  @Test
  public void testFunctionFactory()
    {
    DateParser dateParser = Fluid.function()
      .DateParser().
        fieldDeclaration( Fields.size( 1 ) ).
        dateFormatString( "" )
      .end();

    assertNotNull( dateParser );
    assertNotNull( dateParser.getFieldDeclaration() );
    assertNotNull( dateParser.getDateFormatString() );
    }

/*
  public static void functions( String[] args )
    {
    Function function = OperationGenerator.build( null )
      .function()
      .DateFormatter()
      .fieldDeclaration( Fields.NONE )
      .dateFormatString( "" )
      .end();


    Filter filter = OperationGenerator.build( null )
      .filter().Counter()
      .group( "" ).counter( "" ).increment( 1 )
      .end();

    AssemblyBuilder.Start assembly = AssemblyGenerator.startAssembly( null );

    assembly.startBranch( "incoming" )
      .each( Fields.NONE ).function( function ).outgoing( Fields.NONE )
      .each( Fields.NONE ).filter( filter );

    assembly.startBranch( "incoming" )
      .each( Fields.NONE ).filter( filter )
      .groupBy( Fields.NONE )
      .every( Fields.NONE ).aggregator( null ).outgoing( Fields.NONE );

    assembly.startBranch( "" ).each( Fields.NONE )
      .function().Insert().fieldDeclaration( Fields.ARGS ).values( "" ).end()
      .each( Fields.NONE ).filter( filter )
      .groupBy( Fields.ALL )
      .every( Fields.NONE ).buffer().FirstNBuffer();

    Pipe[] tail = assembly
      .startBranch( "incoming" )
      .each( Fields.NONE ).function().FieldFormatter().format( "" ).end()
      .each( Fields.NONE ).function( function ).outgoing( Fields.NONE )
      .groupBy( Fields.NONE )
      .every( Fields.NONE ).aggregator().Average().fieldDeclaration( Fields.NONE ).end()
      .each( Fields.NONE ).filter( filter )
      .completeBranch().completeAssembly();

    assembly.completeAssembly();
    }
*/

  }
