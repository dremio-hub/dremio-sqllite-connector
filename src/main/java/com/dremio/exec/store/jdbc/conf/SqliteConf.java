/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import org.hibernate.validator.constraints.NotBlank;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.Tag;

/**
 * Configuration for SQLite sources.
 */
@SourceType(value = "SQLITE", label = "SQLite", uiConfig = "sqlite-layout.json")
public class SqliteConf extends AbstractArpConf<SqliteConf> {
  private static final String ARP_FILENAME = "arp/implementation/sqlite-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "org.sqlite.JDBC";

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "Database")
  public String database;

  @Tag(2)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 200;

  @Tag(3)
  @NotMetadataImpacting
  @DisplayMetadata(label = ENABLE_EXTERNAL_QUERY_LABEL)
  public boolean enableExternalQuery = false;

  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String database = checkNotNull(this.database, "Missing database.");

    return String.format("jdbc:sqlite:%s", database);
  }

  @Override
  @VisibleForTesting
  public JdbcPluginConfig buildPluginConfig(
          JdbcPluginConfig.Builder configBuilder,
          CredentialsService credentialsService,
          OptionManager optionManager
  ) {
    return configBuilder.withDialect(getDialect())
            .withDialect(getDialect())
            .withFetchSize(fetchSize)
            .withDatasourceFactory(this::newDataSource)
            .clearHiddenSchemas()
            .addHiddenSchema("SYSTEM")
            .withAllowExternalQuery(enableExternalQuery)
            .build();
  }

  private CloseableDataSource newDataSource() {
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
      toJdbcConnectionString(), null, null, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
  }

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}
