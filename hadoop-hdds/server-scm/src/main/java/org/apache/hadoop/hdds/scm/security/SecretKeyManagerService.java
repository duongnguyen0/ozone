/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.security;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.security.symmetric.LocalSecretKeyStore;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyManager;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyState;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyStore;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_CHECK_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_CHECK_DURATION_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_CA_CERT_STORAGE_DIR;

/**
 * A background service running in SCM to maintain the SecretKeys lifecycle.
 */
public class SecretKeyManagerService implements SCMService, Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(SecretKeyManagerService.class);

  private final SCMContext scmContext;
  private final SecretKeyManager secretKeyManager;


  /**
   * SCMService related variables.
   */
  private final Lock serviceLock = new ReentrantLock();
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;

  private final Duration rotationCheckDuration;
  private final ScheduledExecutorService scheduler;

  @SuppressWarnings("parameternumber")
  public SecretKeyManagerService(SCMContext scmContext,
                                 ConfigurationSource conf,
                                 SCMRatisServer ratisServer) {
    this.scmContext = scmContext;

    SecretKeyConfig secretKeyConfig = new SecretKeyConfig(conf,
        SCM_CA_CERT_STORAGE_DIR);
    SecretKeyStore secretKeyStore = new LocalSecretKeyStore(
        secretKeyConfig.getLocalSecretKeyFile());
    SecretKeyState secretKeyState = new ScmSecretKeyStateBuilder()
        .setSecretKeyStore(secretKeyStore)
        .setRatisServer(ratisServer)
        .build();
    secretKeyManager = new SecretKeyManager(secretKeyState,
        secretKeyStore, secretKeyConfig);

    scheduler = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat(getServiceName())
            .build());

    long rotationCheckInMs = conf.getTimeDuration(
        HDDS_SECRET_KEY_ROTATE_CHECK_DURATION,
        HDDS_SECRET_KEY_ROTATE_CHECK_DURATION_DEFAULT, TimeUnit.MILLISECONDS);
    rotationCheckDuration = Duration.ofMillis(rotationCheckInMs);

    start();
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      if (scmContext.isLeaderReady()) {
        // Asynchronously initialize SecretKeys for first time leader.
        if (secretKeyManager.isInitialized()) {
          scheduler.schedule(() -> {
            try {
              secretKeyManager.checkAndInitialize();
            } catch (TimeoutException e) {
              throw new RuntimeException(
                  "Timeout replicating initialized state.", e);
            }
          }, 0, TimeUnit.SECONDS);
        }

        serviceStatus = ServiceStatus.RUNNING;
      } else {
        serviceStatus = ServiceStatus.PAUSING;
      }
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public boolean shouldRun() {
    serviceLock.lock();
    try {
      return serviceStatus == ServiceStatus.RUNNING;
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public void run() {
    if (!shouldRun()) {
      return;
    }

    try {
      boolean rotated = secretKeyManager.checkAndRotate();
      if (rotated) {
        LOG.info("SecretKeys have been updated.");
      } else {
        LOG.info("SecretKeys have not been updated.");
      }
    } catch (TimeoutException e) {
      LOG.error("Error occurred when updating SecretKeys", e);
    }
  }

  @Override
  public String getServiceName() {
    return SecretKeyManagerService.class.getSimpleName();
  }

  @Override
  public void start() {
    LOG.info("Scheduling rotation checker with interval {} seconds",
        rotationCheckDuration.toMillis() / 1000);
    scheduler.scheduleAtFixedRate(this, 0, rotationCheckDuration.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    scheduler.shutdownNow();
  }

  public static boolean isSecretKeyEnable(SecurityConfig conf) {
    return conf.isSecurityEnabled() &&
        (conf.isBlockTokenEnabled() || conf.isContainerTokenEnabled());
  }
}
