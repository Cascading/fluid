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

import cascading.pipe.assembly.AggregateBy;
import unquietcode.tools.flapi.Descriptor;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f;

/**
 *
 */
public class SubAssembliesGenerator extends Generator
  {
  public SubAssembliesGenerator()
    {
    }

  public SubAssembliesGenerator( String... packages )
    {
    super( packages );
    }

  public void createOperationBuilder( String targetPath )
    {
    DescriptorBuilder_2m1_4f_2m2_4f_2m3_4f_2m4_4f_2m7_4f_2m8_4f_2m10_4f_2m11_4f<Void> builder = getBuilder()
      .setPackage( "cascading.fluid.api.subassembly" )
      .setDescriptorName( "SubAssembly" )
      .setStartingMethodName( "build" );

    builder = addBuilderBlock( builder, AggregateBy.class, true, AGGREGATE_BY, FACTORY, false );

    Descriptor build = builder.enableCondensedClassNames().build();

    writeBuilder( targetPath, build );
    }

  }
