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

import cascading.fluid.generator.builder.AssemblyGenerator;
import cascading.fluid.generator.builder.OperationsGenerator;
import cascading.fluid.generator.builder.SubAssembliesGenerator;
import cascading.fluid.generator.javadocs.DocsHelper;
import cascading.fluid.generator.javadocs.DocumentationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unquietcode.tools.flapi.Flapi;

import java.io.File;
import java.util.Map;

public class Runner
  {
  private static final Logger LOG = LoggerFactory.getLogger( Main.class );

  private String outputPath;
  private String sourceDir;

  public Runner( File sourceDir, File outputDir )
    {
    this( sourceDir.toString(), outputDir.toString() );
    }

  public Runner( String sourceDir, String outputDir )
    {
    this.sourceDir = sourceDir;
    this.outputPath = outputDir;
    }

  public void execute()
    {
    if( outputPath == null )
      throw new IllegalStateException( "outputPath is null" );

    LOG.info( "generating api to: {}", outputPath );

    // preprocess sources to extract documentation
    Map<String, DocumentationInfo> documentationInfo = new SourceCompiler().processSources( sourceDir );
    DocsHelper documentationHelper = new DocsHelper( documentationInfo );
    LOG.info( "processed {} documentation objects", documentationInfo.size() );

    Flapi.shouldOutputRuntime( true );
    new AssemblyGenerator( documentationHelper ).createAssemblyBuilder( outputPath );
    new OperationsGenerator( documentationHelper ).createOperationBuilder( outputPath );
    new SubAssembliesGenerator( documentationHelper ).createOperationBuilder( outputPath );
    }
  }