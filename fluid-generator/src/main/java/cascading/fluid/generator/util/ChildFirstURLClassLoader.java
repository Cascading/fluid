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

package cascading.fluid.generator.util;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ChildFirstURLClassLoader extends ClassLoader
  {
  private static final Logger LOG = LoggerFactory.getLogger( ChildFirstURLClassLoader.class );

  private final String[] exclusions;
  private ChildURLClassLoader childClassLoader;

  private class ChildURLClassLoader extends URLClassLoader
    {
    private ClassLoader parentClassLoader;

    public ChildURLClassLoader( URL[] urls, ClassLoader parentClassLoader )
      {
      super( urls, null );

      this.parentClassLoader = parentClassLoader;
      }

    @Override
    public Class<?> findClass( String name ) throws ClassNotFoundException
      {
      for( String exclusion : exclusions )
        {
        if( name.startsWith( exclusion ) )
          return parentClassLoader.loadClass( name );
        }

      try
        {
        return super.findClass( name );
        }
      catch( ClassNotFoundException exception )
        {
        return parentClassLoader.loadClass( name );
        }
      }
    }

  public ChildFirstURLClassLoader( URL... urls )
    {
    this( new String[ 0 ], urls );
    }

  public ChildFirstURLClassLoader( String[] exclusions, URL... urls )
    {
    super( Thread.currentThread().getContextClassLoader() );
    this.exclusions = exclusions;

    childClassLoader = new ChildURLClassLoader( urls, this.getParent() );

    if( LOG.isDebugEnabled() )
      LOG.debug( "child first classloader exclusions: {}", Arrays.toString( exclusions ) );
    }

  @Override
  protected synchronized Class<?> loadClass( String name, boolean resolve ) throws ClassNotFoundException
    {
    for( String exclusion : exclusions )
      {
      if( name.startsWith( exclusion ) )
        {
        LOG.debug( "loading exclusion: {}, from parent: {}", exclusion, name );
        return super.loadClass( name, resolve );
        }
      }

    try
      {
      LOG.debug( "loading from child: {}", name );
      return childClassLoader.loadClass( name );
      }
    catch( ClassNotFoundException exception )
      {
      return super.loadClass( name, resolve );
      }
    }
  }
