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

package cascading.fluid.generator;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import cascading.fluid.generator.builder.Generator;
import cascading.fluid.generator.javadocs.DocsHelper;
import cascading.fluid.generator.javadocs.DocumentationInfo;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unquietcode.tools.flapi.Flapi;

public abstract class Runner {
  protected static final Logger LOG = LoggerFactory.getLogger(Main.class);

  private final String outputPath;
  private final String sourceDir;

  public Runner( File sourceDir, File outputDir )
    {
    this(sourceDir.toString(), outputDir.toString());
    }

  public Runner( String sourceDir, String outputDir )
    {
    this.sourceDir = sourceDir;
    this.outputPath = Objects.requireNonNull( outputDir, "outputPath is required" );
    }

  public final void execute()
    {
    execute( null );
    }

  public final void execute( Reflections reflectionHelper )
    {
    LOG.info( "generating api to: {}", outputPath );
    Flapi.shouldOutputRuntime( true );

    // preprocess sources to extract documentation
    Map<String, DocumentationInfo> documentationInfo = new SourceCompiler().processSources( sourceDir );
    DocsHelper documentationHelper = new DocsHelper( documentationInfo );
    LOG.info("processed {} documentation objects", documentationInfo.size());

    // execute with or without a reflective helper
    final List<Generator> generators;

    if (reflectionHelper != null) {
      generators = generators( documentationHelper, reflectionHelper );
    } else {
      generators = generators( documentationHelper );
    }

    for( Generator generator : generators )
      {
      generator.generate( outputPath );
      }
    }

  protected abstract List<Generator> generators( DocsHelper documentationHelper );
  protected abstract List<Generator> generators( DocsHelper documentationHelper, Reflections reflectionHelper );
}