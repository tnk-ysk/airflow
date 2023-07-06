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
from typing import Any, AsyncIterator, Sequence

import aiohttp

from airflow.exceptions import AirflowException
from google.cloud.devtools.cloudbuild_v1.types import Build

from airflow.providers.http.hooks.http import HttpAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class HttpTrigger(BaseTrigger):
    """
    CloudBuildCreateBuildTrigger run on the trigger worker to perform create Build operation.

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
    :param poll_interval: polling period in seconds to check for the status
    :param location: The location of the project.
    """

    def __init__(
        self,
        http_conn_id: str = "http_default",
        auth_type: Any = None,
        method: str = "POST",
        endpoint: str | None = None,
        headers: dict[str, str] | None = None,
        data: Any = None,
        extra_options: dict[str, Any] | None = None,
    ):
        super().__init__()
        self.http_conn_id = http_conn_id
        self.method = method
        self.auth_type = auth_type
        self.endpoint = endpoint
        self.headers = headers
        self.data = data
        self.extra_options = extra_options

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes CloudBuildCreateBuildTrigger arguments and classpath."""
        return (
            "airflow.providers.http.triggers.http.HttpTrigger",
            {
                "http_conn_id": self.http_conn_id,
                "method": self.method,
                "auth_type": self.auth_type,
                "endpoint": self.endpoint,
                "headers": self.headers,
                "data": self.data,
                "extra_options": self.extra_options,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Makes a series of asynchronous http calls via an http hook. It yields a Trigger if
        response is a 200 and run_state is successful, will retry the call up to the retry limit
        if the error is 'retryable', otherwise it throws an exception.
        """
        hook = self._get_async_hook()
        try:
            response = await hook.run(
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                extra_options=self.extra_options,
            )
            yield TriggerEvent({"status": "success", "response": response})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> HttpAsyncHook:
        return HttpAsyncHook(
            method=self.method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
        )
