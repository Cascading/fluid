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

import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.operation.Filter;
import cascading.operation.Function;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unquietcode.tools.flapi.Descriptor;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder_m1_m4_m5;

/**
 *
 */
public class OperationsGenerator extends Generator
  {
  public OperationsGenerator()
    {
    }

  public void createOperationBuilder( String targetPath )
    {
    DescriptorBuilder_m1_m4_m5 builder = getBuilder()
      .setPackage( "cascading.fluid.api.operation" )
      .setDescriptorName( "Operation" )
      .setStartingMethodName( "build" );

    builder = addBuilderBlock( builder, Function.class, true, EACH );
    builder = addBuilderBlock( builder, Filter.class, true, EACH );
    builder = addBuilderBlock( builder, Aggregator.class, true, EACH );
    builder = addBuilderBlock( builder, Buffer.class, true, EACH );

    Descriptor build = builder.enableCondensedClassNames().build();

    writeBuilder( targetPath, build );
    }

  @Override
  protected String getFactoryClass()
    {
    return FACTORY;
    }

  }
