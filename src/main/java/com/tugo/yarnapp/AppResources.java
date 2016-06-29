package com.tugo.yarnapp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * class to handle application resources
 */
public class AppResources
{
  private final Map<String, LocalResource> localResources;
  private final String appDir;

  public AppResources(String appDir)
  {
    this.appDir = appDir;
    localResources = new HashMap<>();
  }

  public void addToLocalResource(FileSystem fs, Path fileSrcPath, String fileDestPath) throws IOException
  {
    String fileName = fileSrcPath.getName();
    String suffix = appDir + "/" + fileName;
    Path dst = new Path(fs.getHomeDirectory(), suffix);
    System.out.println("src " + fileSrcPath  + " dest " + dst);
    fs.copyFromLocalFile(fileSrcPath, dst);
    FileStatus fileStatus = fs.getFileStatus(dst);
    LocalResource lr = LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()),
      LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, fileStatus.getLen(),
      fileStatus.getModificationTime());
    localResources.put(fileDestPath, lr);
  }

  /**
   * copy files from local directory to DFS,
   * populate localResouces accordingly.
   * @return
   * @throws IOException
   */
  public void addLocalResources(String sourceDir) throws IOException
  {
    Path path = new Path(sourceDir);
    FileSystem localfs = FileSystem.newInstance(path.toUri(), new Configuration());
    FileSystem remotefs = FileSystem.newInstance(new Path("hdfs:///user/").toUri(), new Configuration());
    System.out.println("Home directory " + remotefs.getHomeDirectory());
    FileStatus[] files = localfs.listStatus(path);
    for (FileStatus fileStatus : files) {
      System.out.println("name " + fileStatus.getPath().getName());
      addToLocalResource(remotefs, fileStatus.getPath(), fileStatus.getPath().getName());
    }
  }

  /**
   * Assuming that files are already located at DFS
   * @param source
   * @throws IOException
   */
  public void populateResouces(String source) throws IOException
  {
    Path sourcePath = new Path(source);
    FileSystem sourceFs = FileSystem.newInstance(sourcePath.toUri(), new Configuration());
    FileStatus[] files = sourceFs.listStatus(sourcePath);
    for (FileStatus file : files) {
      LocalResource lr = LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(file.getPath()),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, file.getLen(), file.getModificationTime());
      localResources.put(file.getPath().getName(), lr);
    }
  }

  public void populateResouces() throws IOException
  {
    populateResouces(appDir);
  }

  public Map<String, LocalResource> getLocalResouces() {
    return localResources;
  }
}
