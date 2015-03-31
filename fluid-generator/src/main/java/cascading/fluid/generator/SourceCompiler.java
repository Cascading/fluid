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

package cascading.fluid.generator;

import cascading.fluid.generator.javadocs.DocumentationInfo;
import cascading.fluid.generator.javadocs.DocumentationScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SourceCompiler
  {
  private static final Logger LOG = LoggerFactory.getLogger( SourceCompiler.class );

  public Map<String, DocumentationInfo> processSources( String sourceDir )
    {

    // retrieve the documentation info
    List<DocumentationInfo> collectedInfo = DocumentationScanner.scan( sourceDir );
    LOG.debug( "created {} info objects", collectedInfo.size() );

    // build a mapping of FQCN to info objects
    Map<String, DocumentationInfo> map = new HashMap<>();

    for( DocumentationInfo info : collectedInfo )
      {
      map.put( info.typeFQCN, info );
      }

    return map;
    }

/*	private Map<String, DocumentationInfo> compileAndWriteClasses( URLClassLoader classLoader, String sourceDir ) {

		if (sourceDir == null || sourceDir.trim().isEmpty()) {
			LOG.warn("no sources provided");
			return new HashMap<>();
		}

		List<String> options = new ArrayList<>();
//		options.add("-classpath");
//		options.add(makeClasspath(classLoader));
		options.add("-source");
		options.add("1.8");
		options.add("-target");
		options.add("1.8");

		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
		Iterable<? extends JavaFileObject> compilationUnits = getSourceFiles(fileManager, sourceDir);

		DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
		JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, options, null, compilationUnits);
        task.call();

		try {
			fileManager.close();
		} catch (IOException e) {
			// nothing
		}

		boolean atLeastOneError = false;

		for (Diagnostic<? extends JavaFileObject> error : diagnostics.getDiagnostics()) {

			if (error.getKind() != Diagnostic.Kind.NOTE) {
				StringBuilder message = new StringBuilder()
					.append(error.getSource().getName())
					.append(" (").append(error.getLineNumber()).append(",").append(error.getColumnNumber()).append(")\n")
					.append(error.getMessage(Locale.getDefault()));

				System.err.println(message.toString());
				atLeastOneError = true;
			}
		}

		if (atLeastOneError) {
//			throw new RuntimeException("The compilation was completed with errors.");
		}

        return processDocumentationInfo(task);
    }*/

/*
    private Map<String, DocumentationInfo> processDocumentationInfo(JavaCompiler.CompilationTask task) {

        // get the tree provider for this compilation task
        DocTrees treeProvider = DocTrees.instance(task);

        // get the root tree
        DocCommentTree tree = treeProvider.getDocCommentTree(null);

        // create a new scanner/visitor, scan the tree
        DocumentationScanner scanner = new DocumentationScanner();
//        scanner.scan(tree, null);

        // retrieve the documentation info
        List<DocumentationInfo> collectedInfo = scanner.getCollectedInfo();

        // build a mapping of FQCN to info objects
        Map<String, DocumentationInfo> map = new HashMap<>();

        for (DocumentationInfo info : collectedInfo) {
            map.put(info.typeFQCN, info);
        }

        return map;
    }
*/



/*
    private static Iterable<? extends JavaFileObject> getSourceFiles(StandardJavaFileManager fileManager, String sourceDir) {
		List<String> fileNames = new ArrayList<>();

		// read all files recursively in the directory
		Iterable<File> files = Files.fileTreeTraverser().preOrderTraversal(new File(sourceDir));

		for (File file : files) {
			if (file.getName().endsWith(".java")) {
				fileNames.add(file.toString());
			}
		}

		LOG.info("{} files for path {}", fileNames.size(), sourceDir);
		return fileManager.getJavaFileObjectsFromStrings(fileNames);
	}
*/
/*
    private static String makeClasspath(URLClassLoader classLoader) {
		StringBuilder buffer = new StringBuilder("\"");

		for (URL url : classLoader.getURLs()) {
			final File file;
			try {
				file = new File(url.toURI());
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			}

			buffer.append(file);
			buffer.append(System.getProperty("path.separator"));
		}

		return buffer.append("\"").toString();
	}*/
  }
