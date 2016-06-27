package com.tugo.yarnapp.master;

import org.apache.hadoop.service.AbstractService;

public class MyAppService extends AbstractService
{
  public MyAppService() {
    super("MyAppService");
  }


  boolean run() {
    return false;
  }
}
