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
"""This module contains Google Cloud Batch operators."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.batch_v1.types.job import Job
from google.cloud.batch_v1.types.task import Task

from airflow.providers.google.cloud.links.cloud_batch import (
    CloudBatchJobListLink,
    CloudBatchJobDetailsLink,
)
from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.cloud_batch import CloudBatchCreateJobTrigger
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME
from airflow.utils import yaml
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudBatchCreateJobOperator(GoogleCloudBaseOperator):
    template_fields: Sequence[str] = ("project_id", "location", "job_id", "gcp_conn_id")
    operator_extra_links = (
        CloudBatchJobDetailsLink(),
    )

    def __init__(
        self,
        *,
        location: str,
        job_id: str,
        job: dict | Job,
        project_id: str | None = None,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        poll_interval: float = 4.0,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.job_id = job_id
        self.job = job
        # Not template fields to keep original value
        self.job_raw = job
        self.project_id = project_id
        self.wait = wait
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to
        self.poll_interval = poll_interval
        self.deferrable = deferrable

    def prepare_template(self) -> None:
        # if no file is specified, skip
        if not isinstance(self.job_raw, str):
            return
        with open(self.job_raw) as file:
            if any(self.job_raw.endswith(ext) for ext in [".yaml", ".yml"]):
                self.job = yaml.safe_load(file.read())
            if self.job_raw.endswith(".json"):
                self.job = json.loads(file.read())

    def execute(self, context: Context):
        hook = CloudBatchHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        job = hook.create_job(
            location=self.location,
            job_id=self.job_id,
            job=self.job,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.xcom_push(context, key="job_id", value=self.job_id)
        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBatchJobDetailsLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                region=self.location,
                job_id=self.job_id,
            )

        if not self.wait:
            return Job.to_dict(job)

        if self.deferrable:
            self.defer(
                trigger=CloudBatchCreateJobTrigger(
                    location=self.location,
                    job_id=self.job_id,
                    project_id=self.project_id,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    delegate_to=self.delegate_to,
                    poll_interval=self.poll_interval,
                ),
                method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
            )
        else:
            job = hook.wait_for_batch_job(
                location=self.location,
                job_id=self.job_id,
                project_id=self.project_id,
                poll_interval=self.poll_interval,
            )
            return Job.to_dict(job)

    def execute_complete(self, context: Context, event: dict):
        if event["status"] == "success":
            return event["instance"]
        else:
            raise AirflowException(f"Unexpected error in the operation: {event['message']}")


class CloudBatchDeleteJobOperator(GoogleCloudBaseOperator):
    template_fields: Sequence[str] = ("project_id", "location", "job_id", "gcp_conn_id")

    def __init__(
        self,
        *,
        location: str,
        job_id: str,
        project_id: str | None = None,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        poll_interval: float = 4.0,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.job_id = job_id
        self.project_id = project_id
        self.wait = wait
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to
        self.poll_interval = poll_interval

    def execute(self, context: Context):
        hook = CloudBatchHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        cloud_batch_operation = hook.delete_job(
            location=self.location,
            job_id=self.job_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBatchJobListLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
            )

        if self.wait:
            hook.wait_for_operation(
                timeout=self.timeout,
                operation=cloud_batch_operation,
            )


class CloudBatchGetJobOperator(GoogleCloudBaseOperator):
    template_fields: Sequence[str] = ("project_id", "location", "job_id", "gcp_conn_id")
    operator_extra_links = (
        CloudBatchJobDetailsLink(),
    )

    def __init__(
        self,
        *,
        location: str,
        job_id: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.job_id = job_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: Context):
        hook = CloudBatchHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        job = hook.get_job(
            location=self.location,
            job_id=self.job_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBatchJobDetailsLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
                region=self.location,
                job_id=self.job_id,
            )
        return Job.to_dict(job)


class CloudBatchListJobsOperator(GoogleCloudBaseOperator):
    template_fields: Sequence[str] = ("project_id", "location", "gcp_conn_id")
    operator_extra_links = (CloudBatchJobListLink(),)

    def __init__(
        self,
        *,
        location: str,
        project_id: str | None = None,
        filter_: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.filter_ = filter_
        self.page_size = page_size
        self.page_token = page_token
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: Context):
        hook = CloudBatchHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        results = hook.list_jobs(
            location=self.location,
            project_id=self.project_id,
            filter_=self.filter_,
            page_size=self.page_size,
            page_token=self.page_token,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        project_id = self.project_id or hook.project_id
        if project_id:
            CloudBatchJobListLink.persist(
                context=context,
                task_instance=self,
                project_id=project_id,
            )
        return [Job.to_dict(result) for result in results]


class CloudBatchGetTaskOperator(GoogleCloudBaseOperator):
    template_fields: Sequence[str] = ("name", "gcp_conn_id")

    def __init__(
        self,
        *,
        name: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: Context):
        hook = CloudBatchHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        task = hook.get_task(
            name=self.name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return Task.to_dict(task)


class CloudBatchListTasksOperator(GoogleCloudBaseOperator):
    template_fields: Sequence[str] = ("project_id", "location", "job_id", "task_group_", "gcp_conn_id")

    def __init__(
        self,
        *,
        location: str,
        job_id: str,
        task_group_: str,
        project_id: str | None = None,
        filter_: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.job_id = job_id
        self.task_group_ = task_group_
        self.project_id = project_id
        self.filter_ = filter_
        self.page_size = page_size
        self.page_token = page_token
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: Context):
        hook = CloudBatchHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        results = hook.list_tasks(
            location=self.location,
            job_id=self.job_id,
            task_group_=self.task_group_,
            project_id=self.project_id,
            filter_=self.filter_,
            page_size=self.page_size,
            page_token=self.page_token,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [Task.to_dict(result) for result in results]
