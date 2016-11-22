package com.hortonworks.simpleyarnapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements a simple async app master.
 * In real usages, the callbacks should execute in a separate thread or thread pool
 */
public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {
    private Configuration configuration;
    private NMClient nmClient;
    private int numContainersToWaitFor;
    private int splits;
    private int totalItemsToProcess;
    private String dataFilePath;
    private AtomicInteger containerCounter = new AtomicInteger(0);

    public ApplicationMasterAsync(String dataFilePath, int numContainersToWaitFor) {
        System.out.println("[Hello, Alex!]");

        this.dataFilePath = dataFilePath;
        this.numContainersToWaitFor = numContainersToWaitFor;
        this.splits = numContainersToWaitFor;

        initTotalItemsToProcess();

        configuration = new YarnConfiguration();
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
    }

    private void initTotalItemsToProcess() {
        totalItemsToProcess = 0;
        try {
            totalItemsToProcess = HDFSHelper.countLines(new Path(Constants.HDFS_ROOT_PATH + dataFilePath));
            System.out.println("[AM] Total items to process: " + totalItemsToProcess);
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
        }
    }

    private int[] getStartAndEnd(int total, int splits, int current) {
        int startNum = 0;
        int endNum = total;
        int diff = (int) Math.ceil((total % splits) * splits);

        if (splits != 1) {
            int step = total / splits;

            endNum = current * step;
            if (endNum + diff < total) {
                startNum = endNum - step;
            }
            else {
                endNum = total;
                startNum = endNum - step + (current * step - total);
            }
        }

        System.out.println("[AM] StartAndEnd: total " + total + ", splits " + splits + ", current " + current);
        System.out.println("[AM] StartAndEnd: [" + startNum + ", " + endNum + "]");

        return new int[]{startNum, endNum};
    }

    /*
    Note:

    We can add file splitting and creating different file for every worker
    and turn 3 input params (file, start line to process, end line to process)
    just to one param - splitted file for worker with lines only for that worker.

    But in our case with small file I believe it might be omitted
    as not to overload HDFS and NameNode with very small files.
     */
    private String getWorkerParams(int workerNum) {
        int[] startAndEnd = getStartAndEnd(totalItemsToProcess, splits, workerNum);

        return " " + dataFilePath + " " + String.valueOf(startAndEnd[0]) + " " + String.valueOf(startAndEnd[1]) + " ";
    }

    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            int counter = containerCounter.incrementAndGet();
            System.out.println("[AM] Container counter: " + counter);

            try {
                // Launch container by create ContainerLaunchContext
                String workerParams = getWorkerParams(counter);

                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(
                        Collections.singletonList(
                                "$JAVA_HOME/bin/java"
                                        + " -Xms256M -Xmx512M"
                                        + " com.hortonworks.simpleyarnapp.WordCountJob"
                                        + workerParams
                                        + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                                        + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));

                Map<String, String> containerEnv = new HashMap<>();
                containerEnv.put("CLASSPATH", "./*");
                ctx.setEnvironment(containerEnv);

                LocalResource appMasterJar = Records.newRecord(LocalResource.class);
                setupAppMasterJar(Constants.HDFS_MY_APP_JAR_PATH, appMasterJar);
                ctx.setLocalResources(Collections.singletonMap("simple-app.jar", appMasterJar));

                System.out.println("[AM] Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(getConfiguration()).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            synchronized (this) {
                numContainersToWaitFor--;
            }
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onReboot() {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        return 0;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        final String command = args[0];
        final int n = Integer.valueOf(args[1]);

        ApplicationMasterAsync master = new ApplicationMasterAsync(command, n);
        master.runMainLoop();
    }

    public void runMainLoop() throws Exception {
        AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();

        // Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster 0");
        RegisterApplicationMasterResponse response = rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");

        System.out.println("[AM] Current number of nodes in the cluster: " + rmClient.getClusterNodeCount());

        int containerMemory = 256;
        int maxMem = response.getMaximumResourceCapability().getMemory();
        System.out.println("[AM] requested containerMemory " + containerMemory + " and maxMem " + maxMem);
        if (containerMemory > maxMem) {
            containerMemory = maxMem;
        }

        int vCores = 2;
        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        System.out.println("[AM] requested vCores " + vCores + " and maxVCores " + maxVCores);
        if (vCores > maxVCores) {
            vCores = maxVCores;
        }

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(vCores);


        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Make container requests to ResourceManager
        for (int i = 0; i < numContainersToWaitFor; ++i) {
            // no special nodes and racks requests
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            System.out.println("[AM] Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
        }

        System.out.println("[AM] waiting for containers to finish");
        while (!doneWithContainers()) {
            Thread.sleep(100);
        }

        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }
}
