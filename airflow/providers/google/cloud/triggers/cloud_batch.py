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
from __future__ import annotations

import asyncio
import warnings
from typing import Any, AsyncIterator, Sequence

from google.cloud.batch_v1.types.job import Job
from google.cloud.batch_v1.types import JobStatus

from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class CloudBatchCreateJobTrigger(BaseTrigger):
    """
    CloudBuildCreateBuildTrigger run on the trigger worker to perform create Build operation

    :param id_: The ID of the build.
    :param project_id: Google Cloud Project where the job is running
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        location: str,
        job_id: str,
        project_id: str | None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        poll_interval: float = 4.0,
    ):
        super().__init__()
        self.location = location
        self.job_id = job_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes CloudBatchCreateJobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchCreateJobTrigger",
            {
                "location": self.location,
                "job_id": self.job_id,
                "project_id": self.project_id,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "delegate_to": self.delegate_to,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets current build execution status and yields a TriggerEvent"""
        hook = self._get_async_hook()
        while True:
            try:
                # Poll for job execution status
                job = await hook.get_job(
                    location=self.location,
                    job_id=self.job_id,
                    project_id=self.project_id,
                )
                if job.status.state in (JobStatus.State.SUCCEEDED,):
                    yield TriggerEvent(
                        {
                            "instance": Job.to_dict(job),
                            "location": self.location,
                            "job_id": self.job_id,
                            "status": "success",
                            "message": "Job completed",
                        }
                    )
                elif job.status.state in (
                    JobStatus.State.QUEUED,
                    JobStatus.State.SCHEDULED,
                    JobStatus.State.RUNNING,
                ):
                    self.log.info("Job is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                elif job.status.state in (
                    JobStatus.State.DELETION_IN_PROGRESS,
                    JobStatus.State.FAILED,
                ):
                    yield TriggerEvent({"status": "error", "message": f"Job state is {job.status.state}"})
                else:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Unidentified status of Cloud Batch Job: {job.status.state}",
                        }
                    )

            except Exception as e:
                self.log.exception("Exception occurred while checking for Cloud Build completion")
                yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> CloudBatchAsyncHook:
        return CloudBatchAsyncHook(gcp_conn_id=self.gcp_conn_id)
