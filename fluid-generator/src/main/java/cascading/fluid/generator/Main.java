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

package cascading.fluid.generator;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.Set;

import cascading.fluid.generator.builder.AssemblyGenerator;
import cascading.fluid.generator.builder.OperationsGenerator;
import cascading.fluid.generator.util.ChildFirstURLClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Main
  {
  private static final Logger LOG = LoggerFactory.getLogger( Main.class );

  private String outputPath;

  public static void main( String[] args )
    {
    String targetPath = args[ 0 ];

    LOG.info( "using classloader: {}", args.length == 2 );

    if( args.length == 2 )
      runViaClassLoader( targetPath, args[ 1 ] );
    else
      new Main( targetPath ).execute();
    }

  public Main( File outputPath )
    {
    this.outputPath = outputPath.toString();
    }

  public Main( String outputPath )
    {
    this.outputPath = outputPath;
    }

  public void execute()
    {
    if( outputPath == null )
      throw new IllegalStateException( "outputPath is null" );

    LOG.info( "generating api to: {}", outputPath );
    new AssemblyGenerator().createAssemblyBuilder( outputPath );
    new OperationsGenerator().createOperationBuilder( outputPath );
    }

  private static void runViaClassLoader( String outputPath, String classpath )
    {
    String[] paths = classpath.split( File.pathSeparator );

    Set<File> files = new LinkedHashSet<File>();

    for( String path : paths )
      files.add( new File( path ) );

    runViaClassLoader( outputPath, files );
    }

  public static void runViaClassLoader( String outputPath, Set<File> files )
    {
    runViaClassLoader( new File( outputPath ), files );
    }

  public static void runViaClassLoader( File outputPath, Set<File> files )
    {
    URL[] urls = new URL[ files.size() ];

    int count = 0;
    for( File file : files )
      {
      LOG.info( "classpath: {}", file );
      urls[ count++ ] = toURL( file.toURI() );
      }

    ChildFirstURLClassLoader urlClassLoader = new ChildFirstURLClassLoader( urls );

    invoke( outputPath, urlClassLoader );
    }

  private static void invoke( File outputPath, ChildFirstURLClassLoader urlClassLoader )
    {
    try
      {
      Class<?> type = urlClassLoader.loadClass( Main.class.getName() );

      Constructor<?> constructor = type.getConstructor( File.class );

      Object value = constructor.newInstance( outputPath );

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
      exception.printStackTrace();
      }
    catch( InstantiationException exception )
      {
      exception.printStackTrace();
      }
    catch( IllegalAccessException exception )
      {
      exception.printStackTrace();
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
