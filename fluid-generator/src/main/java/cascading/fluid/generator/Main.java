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

package cascading.fluid.generator;

import java.io.File;

import cascading.fluid.generator.builder.AssemblyGenerator;
import cascading.fluid.generator.builder.OperationsGenerator;
import cascading.fluid.generator.util.ClassLoaderRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Main
  {
  private static final Logger LOG = LoggerFactory.getLogger( Main.class );

  private String outputPath;

  public static void main( String[] args )
    {
    String targetPath = args[ 0 ];

    LOG.info( "using classloader: {}", args.length == 2 );

    if( args.length == 2 )
      ClassLoaderRunner.runViaClassLoader( args[ 1 ], Main.class.getName(), targetPath );
    else
      new Main( targetPath ).execute();
    }

  public Main( File outputPath )
    {
    this.outputPath = outputPath.toString();
    }

  public Main( String outputPath )
    {
    this.outputPath = outputPath;
    }

  public void execute()
    {
    if( outputPath == null )
      throw new IllegalStateException( "outputPath is null" );

    LOG.info( "generating api to: {}", outputPath );
    new AssemblyGenerator().createAssemblyBuilder( outputPath );
    new OperationsGenerator().createOperationBuilder( outputPath );
    }

  }
