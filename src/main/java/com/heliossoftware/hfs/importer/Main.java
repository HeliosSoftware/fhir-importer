package com.heliossoftware.hfs.importer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.http.HTTPConduit;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.*;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {

  private static String directory;

  private static String fhirUrl;

  private static String contentType;

  private static String fileNameFilter;

  private static int threads;

  private static int returnCode = 0;

  private static volatile AtomicInteger count = new AtomicInteger(0);

  public static void main(String[] argv) throws Exception {

    long startTime = System.currentTimeMillis();
    Args args = new Args();
    JCommander jct = JCommander.newBuilder()
            .addObject(args)
            .build();
    jct.parse(argv);

    directory = args.directory;
    fhirUrl = args.url;
    fileNameFilter = args.fileNameFilter;
    threads =  Integer.parseInt(args.threads);
    contentType = args.contentType;
    if (args.help) {
      jct.usage();
      return;
    }

    new ForkJoinPool(threads).submit(getParallelRunnable()).join();
    long endTime = System.currentTimeMillis();

    System.out.println("\nTime elapsed: " + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss"));

    System.exit(returnCode);

  }

  static Runnable getParallelRunnable() throws Exception {
    FilesystemAccess fsa = new FilesystemAccess();
    Pattern pattern = Pattern.compile(fileNameFilter);
    File output = new File("./output.txt");

    OutputStream outputStream = new FileOutputStream(output);
    WebClient client = WebClient.create(fhirUrl);
    client.header(HttpHeaders.CONTENT_TYPE, contentType);
    client.path("/fhir");
    HTTPConduit conduit = WebClient.getConfig(client).getHttpConduit();
    conduit.getClient().setReceiveTimeout(0);

    return () -> {
      try {
        List<File> files = fsa.getFilesForFolder(directory).parallelStream()
                .filter(file -> fileNameFilter.isEmpty() || pattern.matcher(file.getPath()).find()).collect(Collectors.toList());
        if (files.isEmpty()) {
          throw new IllegalStateException("No files found in directory " + directory);
        }
        System.out.println("Submitting " + files.size() + " files");
        System.out.print("\rStatus: " + Main.count.get() * 100 / files.size() + "% [" + "_".repeat(20) + "]");
        files.parallelStream()
                .forEach(submitFile(client, outputStream, files.size()));
      } catch (IOException e) {
        e.printStackTrace();
        returnCode = 1;
      }
    };
  }

  static Function<File, Response> modifyWebClient(File file, WebClient client) {
    if (file.toPath().getParent().equals(Paths.get(directory))) {
      return client::post;
    } else {
      String fhirPath = "/fhir/" + file.toPath().getParent().getFileName() + "/" + file.getName().split("\\.")[0];
      WebClient newClient = WebClient.create(fhirUrl);
      newClient.header(HttpHeaders.CONTENT_TYPE, contentType);
      newClient.header("Keep-Alive", "timeout=1800");
      newClient.replacePath(fhirPath);
      HTTPConduit conduit = WebClient.getConfig(client).getHttpConduit();
      conduit.getClient().setReceiveTimeout(0);
      return newClient::put;
    }
  }


  static Consumer<File> submitFile(WebClient client, OutputStream outputStream, int size) throws FileNotFoundException {
    return file -> {
      Response response = modifyWebClient(file, client).apply(file);
      if (response.getStatus() != 200 && response.getStatus() != 201) {
        System.out.println("ERROR for file " + file.getPath() + " " + response.getStatus());
        returnCode = 1;
      }
      try {
        outputStream.write(((InputStream) response.getEntity()).readAllBytes());
        outputStream.write('\n');
        Main.count.getAndIncrement();
        int percentage = Main.count.get() * 100 / size;
        System.out.print("\rStatus: " + percentage + "% [" + "=".repeat(percentage / 5) + "_".repeat(20 - percentage / 5) + "]");
      } catch (IOException e) {
        System.err.println("Error writing to file");
        returnCode = 1;
      }
    };
  }

  static class Args {
    @Parameter(names = {"-d", "-dir", "-directory", "-path"}, description = "Path to directory of Resource files")
    private String directory = "./";

    @Parameter(names = {"-t", "-threads"}, description = "Number of threads")
    private String threads = "20";

    @Parameter(names = {"-url", "-server"}, description = "URL of FHIR Server, i.e. http://localhost:8181")
    private String url = "http://localhost:8181";

    @Parameter(names = {"-regex", "-filter"}, description = "File name regex")
    private String fileNameFilter = ".*.json";

    @Parameter(names = {"-c", "-contentType"}, description = "Content Type")
    private String contentType = "application/fhir+json";

    @Parameter(names = {"--help", "-help", "-h"}, description = "This help menu")
    private boolean help = false;
  }
}
