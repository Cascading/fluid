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

package cascading.fluid.generator.javadocs;

import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unquietcode.tools.flapi.builder.Method.MethodBuilder;

/**
 * @author Ben Fagin
 */
public class DocsHelper
  {
  private static final Logger LOG = LoggerFactory.getLogger( DocsHelper.class );
  private final Map<String, DocumentationInfo> documentationInfo;

  public DocsHelper( Map<String, DocumentationInfo> documentationInfo )
    {
    this.documentationInfo = Objects.requireNonNull( documentationInfo );
    }

  public DocumentationInfo lookupDocumentation( Class<?> type )
    {
    return lookupDocumentation( type.getName() );
    }

  public DocumentationInfo lookupDocumentation( String fqcn )
    {
    return documentationInfo.get( fqcn );
    }

  public void addDocumentation( MethodBuilder.Start<?> builder, Class<?> type )
    {
    addDocumentation( builder, type.getName() );
    }

  public void addDocumentation( MethodBuilder.Start<?> builder, String fqcn )
    {
    DocumentationInfo info = lookupDocumentation( fqcn );

    if( info != null )
      {
      builder.withDocumentation( info.docString );
      LOG.trace( "added documentation to type '{}'", fqcn );
      }
    else
      {
      LOG.debug( "no documentation found for type '{}'", fqcn );
      }
    }

  public void addDocumentation( MethodBuilder.Start<?> builder, Class<?> type, String methodSignature )
    {
    addDocumentation( builder, type.getName(), methodSignature );
    }

  public void addDocumentation( MethodBuilder.Start<?> builder, String fqcn, String methodSignature )
    {
    DocumentationInfo info = lookupDocumentation( fqcn );

    if( info != null )
      {
      String methodDocs = info.methodDocs.get( methodSignature );

      if( methodDocs != null )
        {
        builder.withDocumentation( methodDocs );
        LOG.debug( "added documentation to method '{}' of type '{}'", methodSignature, fqcn );
        }

      // fall back to class docs
      else
        {
        LOG.debug( "no documentation found for method '{}' of type '{}', so class docs will be used instead", methodSignature, fqcn );
        builder.withDocumentation( info.docString );
        }
      }
    else
      {
      LOG.debug( "no documentation found for type '{}'", fqcn );
      }
    }
  }
