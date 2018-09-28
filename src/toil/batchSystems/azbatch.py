"""
Implementation of the Azure Batch backend for Toil.

The mapping of the Toil Jobs to Azure Batch semantics is as follows:
- A new Azure Batch Job is created for each instance of AzBatchSystem.
- Each Toil Job issues a Azure Batch Task (Cloud Task) under the Azure Batch Job.
- Resource requests are not currently honored. The resource management can
  handled by having different pool sizes and task scheduling policy.
"""

from __future__ import print_function

import os
import uuid
import datetime
import logging
from threading import Lock
from urlparse import urlparse
from collections import namedtuple

import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
import azure.storage.blob as azureblob
import pytz

from toil.batchSystems.abstractBatchSystem import (BatchSystemSupport)

try:
    input = raw_input # pylint: disable=locally-disabled, invalid-name
except NameError:
    pass

logger = logging.getLogger(__name__) # pylint: disable=locally-disabled, invalid-name

class AzBatchSystem(BatchSystemSupport):

    """
    Implementation of a Toil Batch System for Azure Batch.
    """

    def __init__(self, maxCores, maxMemory, maxDisk, config):
        """
        initializes the batch client and creates the pool.
        """
        super(AzBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)

        exec_options = AzBatchSystem.optionsFromEnvVars()

        self.executor = AzBatchExecutor(exec_options)
        self.user_script = None

    @classmethod
    def optionsFromEnvVars(cls):
        """
        TODO
        """
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
        return True

    # noinspection PyMethodParameters
    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    def setUserScript(self, userScript):
        #raise NotImplementedError()
        self.user_script = userScript

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

        logger.debug('issueBatchJob task id:%s', task_id)
        return task_id

    def killBatchJobs(self, jobIDs):
        """
        Kills the given job IDs.

        :param jobIDs: list of IDs of jobs to kill
        :type jobIDs: list[int]
        """
        for job_id in jobIDs:
            self.executor.cancelAzTask(job_id)

    def getIssuedBatchJobIDs(self):
        """
        Gets all currently issued jobs

        :return: A list of jobs (as jobIDs) currently issued (may be running, or may be
                 waiting to be run). Despite the result being a list, the ordering should not
                 be depended upon.
        :rtype: list[str]
        """
        task_infos = self.executor.getAzTaskInfo()

        return [task_info.id for task_info in task_infos]

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
        keys = [task_info.id for task_info in task_infos]
        durations = [float(task_info.duration.total_seconds()) for task_info in task_infos]
        results = dict(zip(keys, durations))
        logger.debug('getRunningBatchJobIDs %s', results)
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
        logger.debug('getUpdatedBatchJob %s', result)
        return result

    def shutdown(self):
        """
        Called at the completion of a toil invocation.
        Should cleanly terminate all worker threads.
        """
        logger.debug('shutdown')

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

        :param setOption: A function with signature:
           setOption(varName, parsingFn=None, checkFn=None, default=None)
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

class AzBatchExecutor(object):
    """
    TODO
    """
    def __init__(self, options):
        """
        initializes the batch client and creates the pool.
        """
        credentials = batchauth.SharedKeyCredentials(options.batch_account_name,
                                                     options.batch_account_key)

        self.batch_client = batch.BatchServiceClient(credentials,
                                                     options.batch_account_url)

        self.storage_util = AzStorageUtil(options.storage_account_name,
                                          options.storage_account_key,
                                          options.storage_container,
                                          options.storage_account_suffix)
        self.pool_id = options.pool_id
        self.job_id = self.__create_azbatch_job()
        self.lock = Lock()
        self.env_vars = {}
        self.completed_aztasks = []
        self.last_polled = utcnow()
        self.issued_aztasks = {}

    def __create_azbatch_job(self):
        """
        Creates a new azure batch job pool.

        """
        job_id = uuid.uuid4().hex
        logger.info('Creating azure batch job %s in pool: %s', job_id, self.pool_id)
        pool_info = batch.models.PoolInformation()
        pool_info.pool_id = self.pool_id
        job = batch.models.JobAddParameter(
            id=job_id,
            pool_info=pool_info)

        try:
            self.batch_client.job.add(job)
        except batchmodels.batch_error.BatchErrorException as err:
            print_batch_exception(err)
            raise

        logger.info('Job %s created', job_id)

        return job_id

    def popCompletedAzTask(self):
        """
        Returns a single completed task
        rtype: AzTaskExecutionInfo
        """
        #TODO: Refactor this implementation to remove the dependency with pytz
        with self.lock:
            now = utcnow()
            start = self.last_polled
            fmt = "(stateTransitionTime gt datetime'%Y-%m-%dT%H:%M:%S+00:00') and (state eq 'completed')"
            qry = start.strftime(fmt)
            task_infos = self.getAzTaskInfo(qry)
            items = [(int(t.id), t.exitCode, float(t.duration.total_seconds())) for t in task_infos]

            for item in items:
                if int(item[0]) in self.issued_aztasks:
                    self.completed_aztasks.append(item)
                    del self.issued_aztasks[int(item[0])]

            self.last_polled = now
            if not self.completed_aztasks:
                return None

            return self.completed_aztasks.pop(0)

    def addUpdateEnvVar(self, name, value):
        """
        Adds to or updates the list of env variables that will be assigned to all tasks

        param: name a str, environment variable name

        param: value a str, environment variable value
        """

        with self.lock:
            self.env_vars[name] = value

    def cancelAzTask(self, task_id, timeout=30):
        """
        Cancels a running tasks.

        params: task_id a int, id of the task to be cancelled.

        params: timout a int, default 30, timeout for cancellation request.
        """

        terminate_options = batchmodels.TaskTerminateOptions(timeout=timeout)
        self.batch_client.task.terminate(
            self.job_id,
            task_id,
            terminate_options)

    def getAzTaskInfo(self, task_filter=None):
        """
        Gets a list of AzTaskExecutionInfo by the provided filter

        :params string filter:OData filter

        :return: items that match the filter

        :rtype:[AzTaskExecutionInfo]
        """

        #TODO: the max num of results is 1000. This needs to be configurable.
        options = batchmodels.TaskListOptions(filter=task_filter)

        tasks = self.batch_client.task.list(self.job_id, options)
        results = [aztaskinfo_from_aztask(t) for t in tasks]
        return results



    def addAzTask(self, display_name, command):
        """
        Creates and adds a Batch Task with the specified command.

        :param string display_name: task display name.

        :param string command: command to execute in a compute node.

        :return: a unique task id that can be used to reference the newly issued task

        :rtype: int
        """
        task_id = uuid.uuid4().int & (1<<64)-1

        #remove the local filesystem reference in the command.
        logger.debug("Command: %s", command)
        cmd, resources = self.__parseCmdAndGetResources(task_id, command)

        logger.debug("Parsed Command: %s", cmd)
        logger.debug("Resources: %s", resources)


        task = batchmodels.TaskAddParameter(
            id=task_id,
            command_line=wrap_commands_in_shell([' '.join(cmd)]),
            resource_files=resources,
            display_name=display_name)


        if self.env_vars:
            iter_items = self.env_vars.iteritems()
            env_settings = [batchmodels.EnvironmentSetting(name=k, value=v) for k, v in iter_items]
            task.environment_settings = env_settings

        logger.debug("task %s added to job %s", task_id, self.job_id)

        self.batch_client.task.add(self.job_id, task)
        with self.lock:
            self.issued_aztasks[task_id] = True
        return task_id

    def __parseCmdAndGetResources(self, taskid, command):
        worker_cmd = []
        resources = []
        cmd_items = command.split(' ')
        for cmd in cmd_items:
            resource = self.__getResourceFromURI(taskid, cmd)
            if resource is not None:
                resources.append(resource)
            worker_cmd.append(os.path.basename(cmd))

        return worker_cmd, resources

    def __getResourceFromUserscript(self, taskid, toil_userscript):
        """
        TODO
        """
        if toil_userscript is None:
            return None

        return self.__getResourceFromURI(taskid, toil_userscript.url)

    def __getResourceFromURI(self, taskid, uri):
        """
        TODO
        """
        parsed_uri = urlparse(uri)
        file_name = os.path.basename(parsed_uri.path)
        scheme = parsed_uri.scheme

        if scheme == 'http' or scheme == 'https':
            return batchmodels.ResourceFile(blob_source=uri, file_path=file_name)

        if scheme == 'file':
            prefix = taskid
            blob_name = self.storage_util.upload_file(parsed_uri.path,
                                                      prefix)
            blob_url = self.storage_util.get_url_with_readonly_sas(blob_name)
            logger.info("local file %s will be uploaded.", file_name)
            return batchmodels.ResourceFile(blob_source=blob_url, file_path=file_name)


        return None

class AzStorageUtil(object):
    """
    TODO
    """
    def __init__(self, storage_account_name,
                 storage_account_key, container, storage_account_suffix):
        """
        initializes the batch client and creates the pool.
        """
        self.__container = container
        self.__client = azureblob.BlockBlobService(account_name=storage_account_name,
                                                   account_key=storage_account_key,
                                                   endpoint_suffix=storage_account_suffix)

    def get_url_with_readonly_sas(self, blob_name):
        """
        TODO
        """

        sas_token = self.__client.generate_blob_shared_access_signature(
            self.__container,
            blob_name,
            permission=azureblob.BlobPermissions.READ,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

        sas_url = self.__client.make_blob_url(self.__container,
                                              blob_name,
                                              sas_token=sas_token)

        return sas_url

    def upload_file(self, file_path, prefix, name=None):
        """
        Uploads a local file to an Azure Blob storage container.

        :param str file_path: The local path to the file.
        :rtype: `azure.batch.models.ResourceFile`
        :return: A ResourceFile initialized with a SAS URL appropriate for Batch
        tasks.
        """
        blob_name = name
        if blob_name is None:
            blob_name = os.path.basename(file_path)

        blob_name = "%s/%s" % (prefix, blob_name)
        self.__client.create_blob_from_path(self.__container,
                                            blob_name,
                                            file_path)

        return blob_name

def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')

def wrap_commands_in_shell(commands):
    """Wrap commands in a shell

    :param list commands: list of commands to wrap
    :rtype: str
    :return: a shell wrapping commands
    """
    return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(
        ';'.join(commands))

def aztaskinfo_from_aztask(task):
    """
    returns a AzTaskInfo instance from a azure batch task (CloudTask)
    :param azure.batch.models.CloudTask task: source azure batch task

    :return: Task info instance

    :rtype:[AzTaskExecutionInfo]
    """
    now = utcnow()

    duration = now - task.creation_time
    exit_code = None
    if task.execution_info is not None:
        exit_code = task.execution_info.exit_code
    return AzTaskExecutionInfo(int(task.id),
                               duration,
                               exit_code,
                               task.state)
def utcnow():
    """
    TODO
    """
    return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)

AzExecutorOptions = namedtuple('AzExecutorOptions', (
    'pool_id',
    'batch_account_name',
    'batch_account_key',
    'batch_account_url',
    'storage_account_name',
    'storage_account_key',
    'storage_container',
    'storage_account_suffix'))

AzTaskExecutionInfo = namedtuple('AzTaskExecutionInfo', (
    'id',
    #timedelta
    'duration',
    'exitCode',
    'state'))
