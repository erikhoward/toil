"""
TODO
"""
import os
import datetime

import azure.storage.blob as azureblob

class AzStorageUtil():
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
