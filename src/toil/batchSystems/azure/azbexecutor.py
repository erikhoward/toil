from __future__ import print_function
import os
import sys
import time
import uuid
from urlparse import urlparse
from collections import namedtuple

import datetime
import pytz
import logging


try:
    input = raw_input
except NameError:
    pass

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
import azstorage
from threading import Lock


from toil.batchSystems.abstractBatchSystem import (AbstractScalableBatchSystem,
                                                   BatchSystemLocalSupport,
                                                   NodeInfo)
log = logging.getLogger(__name__)

class AzBatchExecutor():
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

        self.storage_util = azstorage.AzStorageUtil(options.storage_account_name,
                                                    options.storage_account_key,
                                                    options.storage_container,
                                                    options.storage_account_suffix)
        self.pool_id = options.pool_id
        self.job_id = self.__create_azbatch_job()
        self.lock = Lock()
        self.env_vars = {}
        self.completedAzTasks = []
        self.lastPolled = utcnow()
        self.issuedAzTasks = {}

       
    def __create_azbatch_job(self):
        """
        Creates a new azure batch job pool.

        """
        
        job_id = uuid.uuid4().hex
        log.info('Creating azure batch job %s in pool: %s' % (job_id, self.pool_id))
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
        
        log.info('Job %s created' % job_id)

        return job_id

    def popCompletedAzTask(self):
        """
        Returns a single completed task
        rtype: AzTaskExecutionInfo
        """
        #TODO: Refactor this implementation to remove the dependency with pytz
        with self.lock:
            now = utcnow()
            start = self.lastPolled
            format_str = "(stateTransitionTime gt datetime'%Y-%m-%dT%H:%M:%S+00:00') and (state eq 'completed')"
            qry = start.strftime(format_str)
            task_infos = self.getAzTaskInfo(qry)
            items = map(lambda t : (int(t.id), t.exitCode, float(t.duration.total_seconds())), task_infos)

            for item in items:
                if int(item[0]) in self.issuedAzTasks:
                    self.completedAzTasks.append(item)
                    del self.issuedAzTasks[int(item[0])]

            self.lastPolled = now
            if len(self.completedAzTasks) == 0:
                return None

            return self.completedAzTasks.pop(0)

    def addUpdateEnvVar(self, name, value):
        """
        Adds to or updates the list of env variables that will be assigned to all tasks

        param: name a str, environment variable name

        param: value a str, environment variable value 
        """

        with self.lock:
            self.env_vars[name] =  value
            
    def cancelAzTask(self, task_id, timeout=30):
        """
        Cancels a running tasks.

        params: task_id a int, id of the task to be cancelled.

        params: timout a int, default 30, timeout for cancellation request.
        """

        terminate_options = batchmodels.TaskTerminateOptions(timeout = timeout) 
        
        self.batch_client.task.terminate(
        self.job_id,
        task_id,
        terminate_options)

    def getAzTaskInfo(self, filter=None):
        """
        Gets a list of AzTaskExecutionInfo by the provided filter

        params:filter a str, ODATA format

        return: items that match the filter

        rtype:[AzTaskExecutionInfo]
        """

        #TODO: the max num of results is 1000. This needs to be configurable. 
        options = batchmodels.TaskListOptions(filter=filter)
    
        tasks = self.batch_client.task.list(self.job_id, options)
        results = map(lambda t : fromCloudTask(t), tasks)
        return results



    def addAzTask(self, displayName, command):
        """
        Creates and adds a Batch Task for executiong a task for with the specified command and returns a task id.

        :param displayName a str, task display name.

        :param command a str, command to execute in a compute node.

        :param userScript a toil.resource. If not None and a valid URI,
        it will uploaded to azure storage from it will be downladed automatically
        to the compute node prior to the execution of the command.

        :return: a unique task id that can be used to reference the newly issued task
        
        :rtype: int
        """
        task_id = uuid.uuid4().int & (1<<64)-1
       
        #remove the local filesystem reference in the command.
        log.debug("Command: %s", command)
        cmd, resources = self.__parseCmdAndGetResources(task_id, command)

        log.debug("Parsed Command: %s", cmd)
        log.debug("Resources: %s", resources)
        

        task = batchmodels.TaskAddParameter(
        id=task_id,
        command_line=wrap_commands_in_shell([' '.join(cmd)]),
        resource_files=resources,
        display_name=displayName)
        
        
        if len(self.env_vars) > 0:
            env_settings = map(lambda (k,v): batchmodels.EnvironmentSetting(name=k, value=v), self.env_vars.iteritems())
            task.environment_settings = env_settings
        
        log.debug("task %s added to job %s", task_id, self.job_id)
        
        self.batch_client.task.add(self.job_id, task)
        self.issuedAzTasks[task_id] = True
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
        fileName = os.path.basename(parsed_uri.path)
        scheme = parsed_uri.scheme
            
        if scheme == 'http' or scheme == 'https' :
            return batchmodels.ResourceFile(blob_source=uri, file_path=fileName)

        if scheme == 'file':
            prefix = taskid
            blob_name = self.storage_util.upload_file(parsed_uri.path,
                                                      prefix)
            blob_url = self.storage_util.get_url_with_readonly_sas(blob_name)
            log.info("local file %s will be uploaded." % (fileName))
            return batchmodels.ResourceFile(blob_source=blob_url, file_path=fileName)


        return None
        
    
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
    
def fromCloudTask(task):
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
    return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
          
AzExecutorOptions = namedtuple('AzExecutorOptions',(
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