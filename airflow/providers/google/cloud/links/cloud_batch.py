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

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

BATCH_BASE_LINK = "/batch"

BATCH_JOB_LIST_LINK = BATCH_BASE_LINK + "/jobs/?project={project_id}"

BATCH_JOB_DETAILS_LINK = BATCH_BASE_LINK + "/jobsDetail/regions/{region}/jobs/{job_id}?project={project_id}"


class CloudBatchJobDetailsLink(BaseGoogleLink):
    """Helper class for constructing Cloud Batch Job Details link"""

    name = "Cloud Batch Job Details"
    key = "cloud_batch_job_details_key"
    format_str = BATCH_JOB_DETAILS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
        region: str,
        job_id: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=CloudBatchJobDetailsLink.key,
            value={
                "project_id": project_id,
                "region": region,
                "job_id": job_id,
            },
        )


class CloudBatchJobListLink(BaseGoogleLink):
    """Helper class for constructing Cloud Batch Jobs List link"""

    name = "Cloud Batch Jobs List"
    key = "cloud_batch_job_list_key"
    format_str = BATCH_JOB_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=CloudBatchJobListLink.key,
            value={
                "project_id": project_id,
            },
        )
