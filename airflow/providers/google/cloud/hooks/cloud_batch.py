#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module contains a Google Cloud Batch Hook."""
from __future__ import annotations

from typing import Tuple, Sequence
from time import sleep

from google.longrunning.operations_pb2 import GetOperationRequest
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.batch_v1 import BatchServiceClient, BatchServiceAsyncClient
from google.cloud.batch_v1.types.job import Job
from google.cloud.batch_v1.types.task import Task
from google.cloud.batch_v1.types import JobStatus

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseHook,
    GoogleBaseAsyncHook,
)


class CloudBatchHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Batch Service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to, impersonation_chain=impersonation_chain
        )
        self._client: BatchServiceClient | None = None

    def get_conn(self) -> BatchServiceClient:
        """
        Retrieves the connection to Google Cloud Batch.

        :return: Google Cloud Batch client object.
        """
        if not self._client:
            self._client = BatchServiceClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def create_job(
        self,
        location: str,
        job_id: str,
        job: dict | Job,
        request_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Job:
        client = self.get_conn()

        parent = f"projects/{project_id}/locations/{location}"

        self.log.info("Start creating job...")

        job = client.create_job(
            request={"parent": parent, "job_id": job_id, "job": job, "request_id": request_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_job(
        self,
        location: str,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        reason: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        client = self.get_conn()

        parent = f"projects/{project_id}/locations/{location}"
        name = f"{parent}/jobs/{job_id}"

        self.log.info(f"Start delete job: {name}.")

        operation = client.delete_job(
            request={"name": name, "request_id": request_id, "reason": reason},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def get_job(
        self,
        location: str,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Job:
        client = self.get_conn()

        parent = f"projects/{project_id}/locations/{location}"
        name = f"{parent}/jobs/{job_id}"

        self.log.info(f"Start get job: {name}.")

        job = client.get_job(
            request={"name": name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info(f"Job has been retrieved: {name}.")

        return job

    @GoogleBaseHook.fallback_to_default_project_id
    def list_jobs(
        self,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        filter_: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> list[Job]:
        client = self.get_conn()

        parent = f"projects/{project_id}/locations/{location}"

        self.log.info(f"Start retrieving jobs.")

        response = client.list_jobs(
            request={"parent": parent, "filter": filter_, "page_size": page_size, "page_token": page_token},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info(f"Jobs have been retrieved.")

        return list(response.jobs)

    def get_task(
        self,
        name: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Task:
        client = self.get_conn()

        self.log.info(f"Start get task: {name}.")

        task = client.get_task(
            request={"name": name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info(f"Task has been retrieved: {name}.")

        return task

    @GoogleBaseHook.fallback_to_default_project_id
    def list_tasks(
        self,
        location: str,
        job_id: str,
        task_group_: str,
        project_id: str = PROVIDE_PROJECT_ID,
        filter_: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> list[Task]:
        client = self.get_conn()

        parent = f"projects/{project_id}/locations/{location}/jobs/{job_id}/taskGroups/{task_group_}"

        self.log.info(f"Start retrieving tasks: {parent}.")

        response = client.list_tasks(
            request={"parent": parent, "filter": filter_, "page_size": page_size, "page_token": page_token},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info(f"Tasks have been retrieved: {parent}.")

        return list(response.tasks)

    def get_operation(self, name: str) -> Operation:
        client = self.get_conn()

        operation = client.get_operation(request=GetOperationRequest(name=name))
        return operation

    def wait_for_batch_job(
        self,
        location: str,
        job_id: str,
        project_id: str,
        poll_interval: float = 4.0,
    ) -> Job:
        while True:
            job = self.get_job(
                location=location,
                job_id=job_id,
                project_id=project_id,
            )
            if job.status.state in (JobStatus.State.SUCCEEDED,):
                return job
            elif job.status.state in (
                JobStatus.State.QUEUED,
                JobStatus.State.SCHEDULED,
                JobStatus.State.RUNNING,
            ):
                self.log.info("Job is still running...")
                self.log.info("Sleeping for %s seconds.", poll_interval)
                sleep(poll_interval)
            elif job.status.state in (
                JobStatus.State.DELETION_IN_PROGRESS,
                JobStatus.State.FAILED,
            ):
                raise AirflowException(f"Job state is {job.status.state}")
            else:
                raise AirflowException(f"Unidentified status of Cloud Batch Job: {job.status.state}")

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)


class CloudBatchAsyncHook(GoogleBaseAsyncHook):
    """Asynchronous Hook for the Google Cloud Batch Service."""

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_job(
        self,
        location: str,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Job:
        """Retrieves a Cloud Batch with a specified name."""
        client = BatchServiceAsyncClient()

        parent = f"projects/{project_id}/locations/{location}"
        name = f"{parent}/jobs/{job_id}"

        job = await client.get_job(
            request={"name": name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job
