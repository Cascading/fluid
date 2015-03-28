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
import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.GroupAssertion;
import cascading.operation.ValueAssertion;
import org.reflections.Reflections;
import unquietcode.tools.flapi.builder.Descriptor.DescriptorBuilder;

public class OperationsGenerator extends Generator
  {
  private String packageName = "cascading.fluid.internal.operation";

  public OperationsGenerator( DocsHelper documentationHelper )
    {
    super( documentationHelper );
    }

  public OperationsGenerator( DocsHelper documentationHelper, Reflections reflections )
    {
    super( documentationHelper, reflections );
    }

  public String getPackageName()
    {
    return packageName;
    }

  public void setPackageName( String packageName )
    {
    this.packageName = packageName;
    }

  @Override
  protected DescriptorBuilder.Start<?> generateInternal( String targetPath )
    {
    DescriptorBuilder.Start<?> builder = getBuilder();

    addBuilderBlock( builder, Function.class, true, EACH, FACTORY, true );
    addBuilderBlock( builder, Filter.class, true, EACH, FACTORY, true );
    addBuilderBlock( builder, Aggregator.class, true, EVERY, FACTORY, true );
    addBuilderBlock( builder, Buffer.class, true, EVERY, FACTORY, true );
    addBuilderBlock( builder, ValueAssertion.class, true, EACH, FACTORY, true );
    addBuilderBlock( builder, GroupAssertion.class, true, EVERY, FACTORY, true );

    builder
      .setPackage( packageName )
      .setDescriptorName( "Operation" )
      .setStartingMethodName( "build" );

    return builder;
    }
  }