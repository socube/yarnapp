package com.tugo.yarnapp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class KillApp
{
  public static void main(String[] args) throws IOException, YarnException
  {
    Configuration conf = new Configuration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    ApplicationId appId = ConverterUtils.toApplicationId(args[0]);
    System.out.println("Application id is " + appId);
    ApplicationReport report = yarnClient.getApplicationReport(appId);
    System.out.println("appReport " + report);
    yarnClient.killApplication(appId);
    yarnClient.stop();
  }
}
