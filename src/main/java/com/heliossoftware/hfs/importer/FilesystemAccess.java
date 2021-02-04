package com.heliossoftware.hfs.importer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FilesystemAccess {

  List<File> getFilesForFolder(String directoryName) throws IOException {
    try (Stream<Path> paths = Files.walk(Paths.get(directoryName))) {
      return paths.filter(Files::isRegularFile).map(Path::toFile).collect(Collectors.toList());
    }
  }
}
