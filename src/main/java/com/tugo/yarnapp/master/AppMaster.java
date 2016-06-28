package com.tugo.yarnapp.master;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;
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

  public void init()
  {
    Map<String, String> envs = System.getenv();
    for(Map.Entry<String, String> entry : envs.entrySet()) {
      System.out.println("key " + entry.getKey() + " val " + entry.getValue());
    }
    conf = new YarnConfiguration();

    ContainerId containerId = ConverterUtils.toContainerId(envs
      .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
    appAttemptID = containerId.getApplicationAttemptId();

    containerMemory = 100;
    containerVirtualCores = 1;
    numTotalContainers = 3;
  }

  void run() throws IOException, YarnException
  {
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

  private void loop() {
    int counter = 0;
    while (true) {
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

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses)
    {

    }

    @Override
    public void onContainersAllocated(List<Container> containers)
    {

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
