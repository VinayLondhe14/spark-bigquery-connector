/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.retrying.RetrySettings;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;

public interface BigQueryConfig {

  Optional<String> getAccessTokenProviderFQCN();

  Optional<String> getCredentialsKey();

  Optional<String> getCredentialsFile();

  Optional<String> getAccessToken();

  String getParentProjectId();

  boolean useParentProjectForMetadataOperations();

  boolean isViewsEnabled();

  Optional<String> getMaterializationProject();

  Optional<String> getMaterializationDataset();

  int getBigQueryClientConnectTimeout();

  int getBigQueryClientReadTimeout();

  RetrySettings getBigQueryClientRetrySettings();

  BigQueryProxyConfig getBigQueryProxyConfig();

  Optional<String> getEndpoint();

  int getCacheExpirationTimeInMinutes();

  ImmutableMap<String, String> getBigQueryJobLabels();

  Optional<Long> getCreateReadSessionTimeoutInSeconds();
}
