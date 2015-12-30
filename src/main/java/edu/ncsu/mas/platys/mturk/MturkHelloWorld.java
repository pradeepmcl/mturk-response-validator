/*
 * Copyright 2007-2012 Amazon Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 * 
 * http://aws.amazon.com/apache2.0
 * 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.ncsu.mas.platys.mturk;

import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;
import com.amazonaws.mturk.requester.Assignment;
import com.amazonaws.mturk.requester.HIT;

/**
 * The MTurk Hello World sample application creates a simple HIT via the
 * Mechanical Turk Java SDK. mturk.properties must be found in the current file
 * path.
 */
public class MturkHelloWorld {

  private RequesterService service;

  private double reward = 0.05;

  /**
   * Constructor
   * 
   */
  public MturkHelloWorld() {
    service = new RequesterService(new PropertiesClientConfig("mturk.properties"));
  }

  /**
   * Check if there are enough funds in your account in order to create the HIT
   * on Mechanical Turk
   * 
   * @return true if there are sufficient funds. False if not.
   */
  public boolean hasEnoughFund() {
    double balance = service.getAccountBalance();
    System.out.println("Got account balance: " + RequesterService.formatCurrency(balance));
    return balance > reward;
  }
  
  public void test() {
    HIT[] hits = service.searchAllHITs();
    for (HIT hit : hits) {
      System.out.println(hit.getHITId() + ", " + hit.getTitle() + ", " + hit.getDescription()
          + ", " + hit.getHITStatus().getValue());
      
      Assignment[] assignments = service.getAllAssignmentsForHIT(hit.getHITId());
      for (Assignment assignment : assignments) {
        System.out.println(assignment.getAssignmentId() + ", " + assignment.getWorkerId() + ", "
            + assignment.getAnswer());
      }
    }
  }

  public static void main(String[] args) {

    MturkHelloWorld app = new MturkHelloWorld();

    if (app.hasEnoughFund()) {
      // app.createHelloWorld();
      app.test();
      System.out.println("Success.");
    } else {
      System.out.println("You do not have enough funds to create the HIT.");
    }
  }
}
