package com.tugo.yarnapp.master;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;
import com.tugo.yarnapp.AppResources;
//import org.apache.hadoop.yarn.api.records.ContainerRA

public class AppMaster
{
  private static final Logger LOG = LoggerFactory.getLogger(AppMaster.class);
  private int containerMemory = 100;
  private int containerVirtualCores = 1;
  private int numTotalContainers = 3;
  private int requestPriority = 0;
  private Configuration conf;
  protected ApplicationAttemptId appAttemptID;

  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;

  // In both secure and non-secure modes, this points to the job-submitter.
  @VisibleForTesting
  UserGroupInformation appSubmitterUgi;

  // Handle to communicate with the Node Manager
  NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  private NMCallbackHandler containerListener;
  private String appMasterHostname;
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";


  AtomicInteger numAllocatedContainers = new AtomicInteger();
  AtomicInteger numCompletedContainers = new AtomicInteger();
  AtomicInteger numFailedContainers = new AtomicInteger();
  AtomicInteger numRequestedContainers = new AtomicInteger();
  private ApplicationId appId;
  private String appName;
  private FileSystem fs;

  public void init() throws IOException
  {
    Map<String, String> envs = System.getenv();
    for(Map.Entry<String, String> entry : envs.entrySet()) {
      System.out.println("key " + entry.getKey() + " val " + entry.getValue());
    }
    conf = new YarnConfiguration();

    ContainerId containerId = ConverterUtils.toContainerId(envs
      .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
    appAttemptID = containerId.getApplicationAttemptId();
    appId = containerId.getApplicationAttemptId().getApplicationId();
    appName = "MyYarnApp";
    containerMemory = 100;
    containerVirtualCores = 1;
    numTotalContainers = 2;
    fs = FileSystem.newInstance(new Configuration());
  }

  void run() throws IOException, YarnException
  {

    Credentials credentials =
      UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    LOG.info("Executing with tokens:");
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info("{}", token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    LOG.info("Starting application master");
    int counter = 0;

    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();

    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response = amRMClient
      .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
        appMasterTrackingUrl);

    List<Container> previousAMRunningContainers =
      response.getContainersFromPreviousAttempts();
    LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
      + " previous attempts' running containers on AM registration.");
    numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

    int numTotalContainersToRequest =
      numTotalContainers - previousAMRunningContainers.size();

    for (int i = 0; i < numTotalContainersToRequest; ++i) {
      AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
      amRMClient.addContainerRequest(containerAsk);
    }
    numRequestedContainers.set(numTotalContainers);

    // wait till completion.
    loop();
  }

  private boolean done = false;

  private void loop() {
    int counter = 0;
    while (!done) {
      LOG.info("counter {}", counter);
      counter++;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private AMRMClient.ContainerRequest setupContainerAskForRM() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    // TODO - what is the range for priority? how to decide?
    Priority pri = Priority.newInstance(requestPriority);

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Resource.newInstance(containerMemory,
      containerVirtualCores);

    AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null,
      pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  @VisibleForTesting
  NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler(this);
  }

  public static void main(String[] args) throws IOException, YarnException
  {
    AppMaster master = new AppMaster();
    master.init();
    master.run();
  }

  private ByteBuffer allTokens;

  private void launchContainer(Container container) throws IOException
  {
    // copy entire application directory.
    Map<String, LocalResource> localResources;
    String appDir = appName + "/" + appId;

    AppResources resources = new AppResources(appDir);
    resources.populateResouces();
    localResources = resources.getLocalResouces();

    // setup command
    ArrayList<CharSequence> vargs = new ArrayList<>();
    vargs.add("/bin/bash client.sh");
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    StringBuilder command = new StringBuilder(9 * vargs.size());
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());

    Map<String, String> shellEnv = new HashMap<>();

    ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
      localResources, shellEnv, commands, null, allTokens.duplicate(), null);
    containerListener.addContainer(container.getId(), container);
    nmClientAsync.startContainerAsync(container, ctx);
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses)
    {
      LOG.info("RMCallbackHandler:onContainersCompleted called with {} containers", statuses.size());
      for (ContainerStatus cs : statuses) {
        LOG.info("Container status id {} state {} exitCode {}", cs.getContainerId(), cs.getState(), cs.getExitStatus());
        int exitStatus = cs.getExitStatus();
        if (exitStatus != 0) {
          if (exitStatus != ContainerExitStatus.ABORTED) {
            LOG.info("Error in container {}", cs.getContainerId());
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
          } else {
            // container failed because of node failure, or framework killed it
            numAllocatedContainers.decrementAndGet();
            numRequestedContainers.decrementAndGet();
          }
        } else {
          // container exited successfully
          numCompletedContainers.incrementAndGet();
          LOG.info("Container completed successfully, container id {}", cs.getContainerId());
        }
      }

      if (numCompletedContainers.get() == numTotalContainers) {
        done = true;
        return;
      }

      // send new request for containers
      int requiredContainers = numTotalContainers - numRequestedContainers.get();
      for(int i = 0; i < requiredContainers; i++) {
        AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
        amRMClient.addContainerRequest(containerAsk);
      }
      numRequestedContainers.set(numTotalContainers);
    }

    @Override
    public void onContainersAllocated(List<Container> containers)
    {
      LOG.info("Allocated containers " + containers.size());
      for(Container c : containers) {
        LOG.info("Allocated container {}", c);
        try {
          launchContainer(c);
        } catch (IOException e) {
          LOG.warn("Unable to launch child container {}", c);
        }
      }
    }

    @Override
    public void onShutdownRequest()
    {

    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes)
    {

    }

    @Override
    public float getProgress()
    {
      return 0;
    }

    @Override
    public void onError(Throwable e)
    {

    }
  }
}
