from __future__ import print_function
import datetime
import os
import sys
import time
import uuid
import logging

try:
    input = raw_input
except NameError:
    pass

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
import azbexecutor
from collections import namedtuple
from azbexecutor import (AzExecutorOptions,
                         AzBatchExecutor)


from toil.batchSystems.abstractBatchSystem import (AbstractScalableBatchSystem,
                                                   BatchSystemSupport,
                                                   NodeInfo)
log = logging.getLogger(__name__)

class AzBatchSystem(BatchSystemSupport):

    """
    Implementation of a Toil Batch System for Azure Batch.
    """

    def __init__(self, maxCores, maxMemory, maxDisk, config):
        """
        initializes the batch client and creates the pool. 
        """
        super(AzBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        
        exec_options = self.__getExectOptionsFromEnvVars()

        self.executor = AzBatchExecutor(exec_options) 
        self.userScript = None

    def __getExectOptionsFromEnvVars(self):
        pool_id = os.getenv("AZBATCH_POOLID")
        batch_account = os.getenv("AZBATCH_ACCOUNT")
        batch_key = os.getenv("AZBATCH_KEY")
        batch_url = os.getenv("AZBATCH_URL")
        storage_account = os.getenv("AZSTORAGE_ACCOUNT")
        storage_key = os.getenv("AZSTORAGE_KEY")
        storage_container = os.getenv("AZSTORAGE_CONTAINER")
        storage_suffix = os.getenv("AZSTORAGE_SUFFIX")

        options = AzExecutorOptions(pool_id,
                                    batch_account,
                                    batch_key,
                                    batch_url,
                                    storage_account,
                                    storage_key,
                                    storage_container,
                                    storage_suffix)
        return options
    # noinspection PyMethodParameters
    @classmethod
    def supportsAutoDeployment(cls):
        return False

    # noinspection PyMethodParameters
    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    def setUserScript(self, userScript):
        raise NotImplementedError()

        
    def issueBatchJob(self, jobNode):
        """
        Issues a job with the specified command to the batch system and returns a unique jobID.

        :param jobNode a toil.job.JobNode

        :return: a unique jobID that can be used to reference the newly issued job
        :rtype: int
        """
        
        task_id = self.executor.addAzTask(
        jobNode.displayName,
        jobNode.command)

      
        log.debug('issueBatchJob task id:%s', task_id)
        return task_id

    def killBatchJobs(self, jobIDs):
        """
        Kills the given job IDs.

        :param jobIDs: list of IDs of jobs to kill
        :type jobIDs: list[int]
        """
        for id in jobIDs:
            self.executor.cancelAzTask(id)
        
    
    def getIssuedBatchJobIDs(self):
        """
        Gets all currently issued jobs

        :return: A list of jobs (as jobIDs) currently issued (may be running, or may be
                 waiting to be run). Despite the result being a list, the ordering should not
                 be depended upon.
        :rtype: list[str]
        """
        task_infos = self.executor.getAzTaskInfo()
        return map(lambda t: t.id, task_infos) 

    def getRunningBatchJobIDs(self):
        """
        Gets a map of jobs as jobIDs that are currently running (not just waiting)
        and how long they have been running, in seconds.

        :return: dictionary with currently running jobID keys and how many seconds they have
                 been running as the value
        :rtype: dict[str,float]
        """
        qry = "state eq 'running'"
        task_infos = self.executor.getAzTaskInfo(qry)
        keys = map(lambda t : t.id, task_infos)
        durations = map(lambda t : float(t.duration.total_seconds()), task_infos)
        results = dict(zip(keys, durations))
        log.debug('getRunningBatchJobIDs %s', results)
        return results

    def getUpdatedBatchJob(self, maxWait):
        """
        Returns a job that has updated its status.

        :param float maxWait: the number of seconds to block, waiting for a result

        :rtype: tuple(str, int) or None
        :return: If a result is available, returns a tuple (jobID, exitValue, wallTime).
                 Otherwise it returns None. wallTime is the number of seconds (a float) in
                 wall-clock time the job ran for or None if this batch system does not support
                 tracking wall time. Returns None for jobs that were killed.
        """
        result = self.executor.popCompletedAzTask()
        log.debug('getUpdatedBatchJob %s', result)
        return result 
       
    def shutdown(self):
        """
        Called at the completion of a toil invocation.
        Should cleanly terminate all worker threads.
        """
        log.debug('shutdown')
        
    def setEnv(self, name, value=None):
        """
        Set an environment variable for the worker process before it is launched. The worker
        process will typically inherit the environment of the machine it is running on but this
        method makes it possible to override specific variables in that inherited environment
        before the worker is launched. Note that this mechanism is different to the one used by
        the worker internally to set up the environment of a job. A call to this method affects
        all jobs issued after this method returns. Note to implementors: This means that you
        would typically need to copy the variables before enqueuing a job.

        If no value is provided it will be looked up from the current environment.
        """
        self.executor.addUpdateEnvVar(name, value)

        
    @classmethod
    def setOptions(cls, setOption):
        """
        Process command line or configuration options relevant to Azure Batch
        The

        :param setOption: A function with signature setOption(varName, parsingFn=None, checkFn=None, default=None)
           used to update run configuration
        """
        setOption("pool_id", None, None, None)
        setOption("batch_account", None, None, None)
        setOption("batch_key", None, None, None)
        setOption("batch_url", None, None, None)
        setOption("storage_account", None, None, None)
        setOption("storage_key", None, None, None)
        setOption("storage_container", None, None, "toil")
        setOption("storage_suffix", None, None, "core.windows.net")
    
