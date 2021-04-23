#!/bin/bash
#
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# The Google Cloud project to use for this tutorial
export PROJECT_ID="<your-project-id>"

# The Compute Engine region to use for running Dataflow jobs
export REGION_ID="<compute-engine-region>"

# define the GCS bucket to use for Dataflow templates and temporary location.
export GCS_BUCKET="<name-of-the-bucket>"

# Name of the service account to use (not the email address)
export PIPELINE_SERVICE_ACCOUNT_NAME="<service-account-name-for-runner>"

# The API Key created in Datadog for making API calls
# https://app.datadoghq.com/account/settings#api
export DATADOG_API_KEY="<your-datadog-api-key>"


######################################
#      DON'T MODIFY LINES BELOW      #
######################################

# The DLP Runner Service account email
export PIPELINE_SERVICE_ACCOUNT_EMAIL="${PIPELINE_SERVICE_ACCOUNT_NAME}@$(echo $PROJECT_ID | awk -F':' '{print $2"."$1}' | sed 's/^\.//').iam.gserviceaccount.com"

# Set an easy name to invoke the sampler module
export DATADOG_PIPELINE_DIR="${PWD}"
alias bq_2_datadog_pipeline="java -jar ${DATADOG_PIPELINE_DIR}/build/libs/crashlytics-logs-to-datadog-all.jar"
