/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools;

import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterStateVerifier {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterStateVerifier.class);
  private String _clusterName;
  private PinotHelixResourceManager _helixResourceManager;
  private String _zkAddress;

  public ClusterStateVerifier(String zkAddress, String clusterName) {
    _zkAddress = zkAddress;
    _clusterName = clusterName;
    _helixResourceManager =  new PinotHelixResourceManager(getControllerConf());
    _helixResourceManager.start();
  }

  private ControllerConf getControllerConf() {
    ControllerConf conf = new ControllerConf();
    conf.setHelixClusterName(_clusterName);
    conf.setZkStr(_zkAddress);
    conf.setTenantIsolationEnabled(false);
    conf.setControllerVipHost("localhost");
    conf.setControllerVipProtocol("http");
    return conf;
  }

  /**
   * return 0 if all the tables are stable
   * @return
   */
  private int isClusterStable(final List<String> tableNames, final long timeoutSec) {

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<Integer> future = executor.submit(new Callable<Integer>() {
      @Override
      public Integer call() {
        return isClusterStable(tableNames);
      }
    });

    Integer isStable = -1;
    try {
      isStable = future.get(timeoutSec, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException ie) {
      /* Handle the interruption. Or ignore it. */
      LOGGER.error("Exception occurred: {}", ie);
    } catch (TimeoutException te) {
      /* Handle the timeout. */
      LOGGER.error("Reach timeout! timeoutSec: {}", timeoutSec);
    } finally {
      if (!executor.isTerminated()) {
        executor.shutdownNow();
      }
    }
    return isStable;
  }

  /**
   * return 0 if all the tables are stable
   * @return
   */
  private Integer isClusterStable(final List<String> tableNames) {
    LOGGER.info("Start checking the stability of all the tables...");
    int isStable = 0;
    for (String tableName : tableNames) {
      isStable = isStable(tableName);
      if (isStable != 0) {
        LOGGER.error("Table {} is not stable. The cluster is not stable.", tableName);
        break;
      }
    }
    return isStable;
  }

  private int isStable(String tableName) {
    IdealState idealState = _helixResourceManager.getHelixAdmin().getResourceIdealState(_clusterName, tableName);
    ExternalView externalView = _helixResourceManager.getHelixAdmin().getResourceExternalView(_clusterName, tableName);
    Map<String, Map<String, String>> mapFieldsIS = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> mapFieldsEV = externalView.getRecord().getMapFields();
    int numDiff = 0;
    for (String segment : mapFieldsIS.keySet()) {
      Map<String, String> mapIS = mapFieldsIS.get(segment);
      Map<String, String> mapEV = mapFieldsEV.get(segment);

      for (String server : mapIS.keySet()) {
        String state = mapIS.get(server);
        if (mapEV == null || mapEV.get(server) == null || !mapEV.get(server).equals(state)) {
          LOGGER.info("Mismatch: segment" + segment + " server:" + server + " state:" + state);
          numDiff = numDiff + 1;
        }
      }
    }
    return numDiff;
  }

  public int verifyClusterState(String tableName, long timeoutSec) {
    List<String> tableNames;

    List<String> allTables = _helixResourceManager.getAllTables();

    if (tableName == null) {
      tableNames = allTables;
    } else {
      tableNames = new ArrayList<>();
      if (allTables.contains(tableName)) {
        tableNames.add(tableName);
      } else {
        LOGGER.error("Error: Table {} doesn't exist.", tableName);
        return -1;
      }
    }
    return isClusterStable(tableNames, timeoutSec);
  }
}
