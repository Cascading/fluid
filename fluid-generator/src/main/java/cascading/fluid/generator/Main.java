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

package cascading.fluid.generator;

import cascading.fluid.generator.util.ClassLoaderRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main
  {
  private static final Logger LOG = LoggerFactory.getLogger( Main.class );

  public static void main( String[] args )
    {
    String sourcePath = args[ 0 ];
    String targetPath = args[ 1 ];

    LOG.info( "using classloader: {}", args.length == 3 );
    LOG.info( "using sourcePath: {}", sourcePath );
    LOG.info( "using targetPath: {}", targetPath );

    if( args.length == 3 )
      ClassLoaderRunner.runViaClassLoader( args[ 2 ], CascadingRunner.class.getName(), sourcePath, targetPath );
    else
      new CascadingRunner( sourcePath, targetPath ).execute();
    }
  }
