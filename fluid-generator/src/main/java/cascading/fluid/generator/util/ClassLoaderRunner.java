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

package cascading.fluid.generator.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 *
 */
public class ClassLoaderRunner
  {
  private static final Logger LOG = LoggerFactory.getLogger( ClassLoaderRunner.class );

  public static void runViaClassLoader( String classpath, String className, String sourcePath, String outputPath )
    {
    String[] paths = classpath.split( File.pathSeparator );

    Set<File> files = new LinkedHashSet<File>();

    for( String path : paths )
      files.add( new File( path ) );

    runViaClassLoader( files, className, sourcePath, outputPath );
    }

  public static void runViaClassLoader( Set<File> files, String className, String sourcePath, String outputPath )
    {
    runViaClassLoader( files, className, new File( sourcePath ), new File( outputPath ) );
    }

  public static void runViaClassLoader( Set<File> files, String className, File sourcePath, File outputPath )
    {
    URL[] urls = new URL[ files.size() ];

    int count = 0;
    for( File file : files )
      {
      LOG.debug( "classpath: {}", file );
      urls[ count++ ] = toURL( file.toURI() );
      }

    ChildFirstURLClassLoader urlClassLoader = new ChildFirstURLClassLoader( urls );

    invoke( urlClassLoader, className, sourcePath, outputPath );
    }

  private static void invoke( ChildFirstURLClassLoader urlClassLoader, String className, File sourcePath, File outputPath )
    {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    try
      {
      Thread.currentThread().setContextClassLoader( urlClassLoader );

      Class<?> type = urlClassLoader.loadClass( className );

      Constructor<?> constructor = type.getConstructor( File.class, File.class );

      Object value = constructor.newInstance( sourcePath, outputPath );

      Method method = type.getMethod( "execute" );

      method.invoke( value );
      }
    catch( ClassNotFoundException exception )
      {
      throw new RuntimeException( exception );
      }
    catch( NoSuchMethodException exception )
      {
      throw new RuntimeException( exception );
      }
    catch( InvocationTargetException exception )
      {
      throw new RuntimeException( exception.getTargetException() );
      }
    catch( InstantiationException exception )
      {
      throw new RuntimeException( exception );
      }
    catch( IllegalAccessException exception )
      {
      throw new RuntimeException( exception );
      }
    finally
      {
      Thread.currentThread().setContextClassLoader( contextClassLoader );
      }
    }

  private static URL toURL( URI file )
    {
    try
      {
      return file.toURL();
      }
    catch( MalformedURLException exception )
      {
      throw new RuntimeException( "could not create URL for: " + file, exception );
      }
    }
  }
