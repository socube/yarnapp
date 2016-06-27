package com.tugo.yarnapp.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

public class AppMaster
{
  private static final Logger LOG = LoggerFactory.getLogger(AppMaster.class);


  public static void main(String[] args)
  {
    boolean result = false;
    System.out.println("Starting Application Master");
    MyAppService service = null;
    try {
      service = new MyAppService();
      service.init(new Configuration());
      service.start();
      result = service.run();
    } catch (Exception ex) {
      System.exit(1);
    } finally {
      if (service != null)
        service.stop();
    }

    if (result) {
      LOG.info("Application master completed");
      System.exit(0);
    } else {
      LOG.info("Application master errored ");
      System.exit(2);
    }
  }
}
