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
import java.util.Arrays;
import java.util.List;

import cascading.fluid.generator.builder.AssemblyGenerator;
import cascading.fluid.generator.builder.Generator;
import cascading.fluid.generator.builder.OperationsGenerator;
import cascading.fluid.generator.builder.SubAssembliesGenerator;
import cascading.fluid.generator.javadocs.DocsHelper;
import org.reflections.Reflections;

/**
 */
public class CascadingRunner extends Runner
  {
  public CascadingRunner( File sourceDir, File outputDir )
    {
    super( sourceDir, outputDir );
    }

  public CascadingRunner( String sourceDir, String outputDir )
    {
    super( sourceDir, outputDir );
    }

  @Override
  protected List<Generator> generators( DocsHelper documentationHelper )
    {
    return Arrays.asList(
      new AssemblyGenerator( documentationHelper ).includeCascading(),
      new OperationsGenerator( documentationHelper ).includeCascading(),
      new SubAssembliesGenerator( documentationHelper ).includeCascading()
    );
    }

  @Override
  protected List<Generator> generators( DocsHelper documentationHelper, Reflections reflectionHelper )
    {
    return Arrays.asList(
      new AssemblyGenerator( documentationHelper, reflectionHelper ).includeCascading(),
      new OperationsGenerator( documentationHelper, reflectionHelper ).includeCascading(),
      new SubAssembliesGenerator( documentationHelper, reflectionHelper ).includeCascading()
    );
    }
  }
