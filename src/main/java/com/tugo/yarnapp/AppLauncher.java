package com.tugo.yarnapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType.file;

public class AppLauncher
{
  private static final Logger LOG = LoggerFactory.getLogger(AppLauncher.class);

  private final YarnClient yarnClient = YarnClient.createYarnClient();
  private final int amPriority = 0;
  private String appName = "MyYarnApp";
  private ApplicationId appId;
  public String javaCmd = "${JAVA_HOME}" + "/bin/java";
  private String applicationType = "TusharApp";
  private Configuration conf;

  public AppLauncher() {
    conf = new Configuration();
    yarnClient.init(conf);
  }

  public void start() {
    yarnClient.start();
  }

  public void stop() {
    yarnClient.stop();
  }

  public void copyFiles() {

  }

  public void startApplication() throws IOException, YarnException
  {
    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info("Got Cluster metric info from ASM, numNodeManagers={}", clusterMetrics.getNumNodeManagers());

    YarnClientApplication newApp = yarnClient.createApplication();
    appId = newApp.getNewApplicationResponse().getApplicationId();
    LOG.info("Application id is {}", appId);

    GetNewApplicationResponse appResponse = newApp.getNewApplicationResponse();
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    int cores = appResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max mem capability of resources in this cluster mem={} core={}", maxMem, cores);

    LOG.info("Setting up application submission context for ASM");
    ApplicationSubmissionContext appContext = newApp.getApplicationSubmissionContext();
    appId = appContext.getApplicationId();
    appContext.setApplicationName(appName);
    appContext.setApplicationType(this.applicationType);

    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    Map<String, LocalResource> localResources = getLocalResources();
    amContainer.setLocalResources(localResources);

    Map<String, String> env = new HashMap<>();
    env.put("HADOOP_USER_NAME", UserGroupInformation.getLoginUser().getUserName());

    /*
    final String CLASS_PATH_SEPARATOR= "<CPS>";
    StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
      .append(CLASS_PATH_SEPARATOR).append("./*");
    String c = conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
    classPathEnv.append(c.trim());
    classPathEnv.append(CLASS_PATH_SEPARATOR).append(
      "./log4j.properties");
    env.put("CLASSPATH", classPathEnv.toString());
    */

    StringBuilder classPathEnv = new StringBuilder("./*");
    String classpath = conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
    for (String c : StringUtils.isBlank(classpath) ? YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH : classpath.split(",")) {
      if (c.equals("$HADOOP_CLIENT_CONF_DIR")) {
        // SPOI-2501
        continue;
      }
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }
    env.put("CLASSPATH", classPathEnv.toString());

    amContainer.setEnvironment(env);

    // setup the command to run
    ArrayList<CharSequence> vargs = new ArrayList<>(30);
    vargs.add("/tmp/mymaster.sh");
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    StringBuilder command = new StringBuilder(9 * vargs.size());
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up app master command \"{}\"", command.toString());
    List<String> commands = new ArrayList<>();
    commands.add(command.toString());
    amContainer.setCommands(commands);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(1024);
    capability.setVirtualCores(1);
    appContext.setResource(capability);

    appContext.setAMContainerSpec(amContainer);
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(amPriority);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    //appContext.setQueue("default");
    yarnClient.submitApplication(appContext);
  }

  void addToLocalResource(FileSystem fs, Path fileSrcPath, String fileDestPath,
    String appId, Map<String, LocalResource> localResources) throws IOException
  {
    String suffix = appName + "/" + appId + "/" + fileDestPath;
    Path dst = new Path(fs.getHomeDirectory(), suffix);
    System.out.println("src " + fileSrcPath  + " dest " + dst);
    fs.copyFromLocalFile(fileSrcPath, dst);
    FileStatus fileStatus = fs.getFileStatus(dst);
    LocalResource lr = LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()),
      LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, fileStatus.getLen(),
      fileStatus.getModificationTime());
    localResources.put(fileDestPath, lr);
  }

  Map<String, LocalResource> getLocalResources() throws IOException
  {
    Map<String, LocalResource> localResources = new HashMap<>();
    Path path = new Path("file:///tmp/tushar/appfiles/");
    FileSystem localfs = FileSystem.newInstance(path.toUri(), new Configuration());
    FileSystem remotefs = FileSystem.newInstance(new Path("hdfs://localhost:9000/user/").toUri(), new Configuration());
    System.out.println("Home directory " + remotefs.getHomeDirectory());
    FileStatus[] files = localfs.listStatus(path);
    for (FileStatus fileStatus : files) {
      System.out.println("name " + fileStatus.getPath().getName());
      addToLocalResource(remotefs, fileStatus.getPath(), fileStatus.getPath().getName(), appId.toString(), localResources);
    }
    return localResources;
  }

  public static void main(String[] args) throws IOException, YarnException
  {
    AppLauncher appClient = new AppLauncher();
    appClient.start();
    appClient.startApplication();
  }
}
