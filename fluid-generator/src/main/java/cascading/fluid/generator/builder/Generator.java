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

package cascading.fluid.generator.builder;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unquietcode.tools.flapi.Descriptor;
import unquietcode.tools.flapi.Flapi;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder;
import unquietcode.tools.flapi.runtime.MethodLogger;

/**
 *
 */
public class Generator
  {
  private static final Logger LOG = LoggerFactory.getLogger( Generator.class );

  public static final String METHOD_ANNOTATION = "cascading.fluid.factory.MethodMeta";
  public static final String FACTORY = "cascading.fluid.factory.Factory";
  public static final String PIPE_FACTORY = "cascading.fluid.factory.PipeFactory";

  public static final int GROUP = 1;
  public static final int EACH = 2;
  public static final int EVERY = 3;

  protected static MethodLogger methodLogger = MethodLogger.from( System.out );

  protected void writeBuilder( String targetPath, Descriptor build )
    {
    new File( targetPath ).mkdirs();

    build.writeToFolder( targetPath );
    }

  protected DescriptorBuilder.$<Void> getBuilder()
    {
    if( LOG.isDebugEnabled() )
      return Flapi.builder( methodLogger );

    return Flapi.builder();
    }
  }
