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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.io.Files;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.Doclet;
import com.sun.javadoc.ExecutableMemberDoc;
import com.sun.javadoc.RootDoc;
import com.sun.tools.javadoc.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class DocumentationScanner
  {
  private static final Logger LOG = LoggerFactory.getLogger( DocumentationScanner.class );
  private static final List<DocumentationInfo> collectedInfo = new ArrayList<>();

  public static synchronized List<DocumentationInfo> scan( String sourceDir )
    {
    collectedInfo.clear();

    List<String> args = getSourceFiles( sourceDir );
    args.add( 0, "-classpath" );
    args.add( 1, classPath() );

    final String[] argsArray = args.toArray( new String[ args.size() ] );

    try
      {
      Main.execute(
        "Fluid Generator",
        writer( LogLevel.ERROR ), writer( LogLevel.WARN ), writer( LogLevel.DEBUG ),
        ScanningDoclet.class.getName(), argsArray
      );

      return new ArrayList<>( collectedInfo );
      }
    finally
      {
      collectedInfo.clear();
      }
    }

  private static List<String> getSourceFiles( String sourceDir )
    {
    List<String> filePaths = new ArrayList<>();

    // read all files recursively in the directory
    Iterable<File> files = Files.fileTreeTraverser().preOrderTraversal( new File( sourceDir ) );

    for( File file : files )
      {
      if( file.getName().endsWith( ".java" ) )
        {
        filePaths.add( file.toString() );
        }
      }

    LOG.info( "{} files for path {}", filePaths.size(), sourceDir );
    return filePaths;
    }

  private enum LogLevel
    {
      DEBUG, INFO, WARN, ERROR
    }

  private static PrintWriter writer( LogLevel level )
    {
    final ByteArrayOutputStream os = new ByteArrayOutputStream();

        switch (level) {
            case ERROR: return new PrintWriter(os) {
                public @Override void flush() {
                    super.flush();
                    LOG.error(os.toString());
                    os.reset();
                }
            };

            case WARN: return new PrintWriter(os) {
                public @Override void flush() {
                    super.flush();
                    LOG.warn(os.toString());
                    os.reset();
                }
            };

            case INFO: return new PrintWriter(os) {
                public @Override void flush() {
                    super.flush();
                    LOG.info(os.toString());
                    os.reset();
                }
            };

            case DEBUG: return new PrintWriter(os) {
                public @Override void flush() {
                    super.flush();
                    LOG.debug(os.toString());
                    os.reset();
                }
            };

            default: throw new IllegalStateException("invalid level");
        }
    }

    // ---------------------------------------------------- //

  public static class ScanningDoclet extends Doclet
    {

    public static boolean start( RootDoc root )
      {
      for( ClassDoc classDoc : root.classes() )
        {
        DocumentationInfo info = new DocumentationInfo( classDoc.qualifiedName(), classDoc.commentText() );

        // merge regular methods and constructor methods
        List<ExecutableMemberDoc> methods = new ArrayList<>();
        methods.addAll( Arrays.asList( classDoc.methods() ) );
        methods.addAll( Arrays.asList( classDoc.constructors() ) );

        for( ExecutableMemberDoc methodDoc : methods )
          {
          String methodName = methodDoc.name() + methodDoc.signature();

          // skip hashcode and equals methods
          if( "equals(java.lang.Object)".equals( methodName ) || "hashCode()".equals( methodName ) )
            {
            continue;
            }

          final String docs = methodDoc.commentText();

          if( docs != null && !docs.trim().isEmpty() )
            {
            info.methodDocs.put( methodName, docs );
            }
          }

        collectedInfo.add( info );
        }

      return true;
      }
    }

  private static String classPath()
    {
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();

    if (!(loader instanceof URLClassLoader))
      {
      LOG.debug( "unsupported classloader type '{}'", loader.getClass().getName() );
      return "";
      }

    return classPath( (URLClassLoader) loader );
    }

  private static String classPath(URLClassLoader loader)
    {
    StringBuilder sb = new StringBuilder();
    boolean first = true;

    for( URL url : loader.getURLs() )
      {
        if (!first) {
          sb.append( File.pathSeparatorChar );
        } else {
          first = false;
        }

        sb.append( url.toString() );
      }

    return sb.toString();
    }
  }