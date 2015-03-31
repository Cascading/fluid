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

package cascading.fluid.generator.builder;

import cascading.fluid.generator.javadocs.DocsHelper;
import cascading.pipe.assembly.AggregateBy;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder;

public class SubAssembliesGenerator extends Generator
  {
  public SubAssembliesGenerator( DocsHelper documentationHelper )
    {
    super( documentationHelper );
    }

  public SubAssembliesGenerator( DocsHelper documentationHelper, String... packages )
    {
    super( documentationHelper, packages );
    }

  public void createOperationBuilder( String targetPath )
    {
    DescriptorBuilder.Start<?> builder = getBuilder();

    addBuilderBlock( builder, AggregateBy.class, true, AGGREGATE_BY, FACTORY, false );

    builder
      .setPackage( "cascading.fluid.internal.subassembly" )
      .setDescriptorName( "SubAssembly" )
      .setStartingMethodName( "build" );

    completeAndWriteBuilder( targetPath, builder );
    }

  }