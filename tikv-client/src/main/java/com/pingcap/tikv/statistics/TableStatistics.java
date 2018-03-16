/*
 *
 * Copyright 2018 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv.statistics;

import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A TableStatistics Java plain object.
 * <p>
 * Usually each table will have two types of statistics information:
 * 1. Meta info
 * (tableId, count, modifyCount, version)
 * 2. Column/Index histogram info
 * (columnsHistMap, indexHistMap)
 */
public class TableStatistics {
  private final TiTableInfo tableInfo; // Which table it belongs to
  private final Map<Long, ColumnStatistics> columnsHistMap = new HashMap<>(); // ColumnId -> ColumnStatistics map
  private final Map<Long, IndexStatistics> indexHistMap = new HashMap<>(); // IndexId -> IndexStatistics map
  private long count; // Total row count in a table.
  private long modifyCount; // Total modify count in a table.
  private long version; // Version of this statistics info
  private transient Snapshot snapshot;
  private transient TiTableInfo metaTable;
  private transient TiTableInfo histTable;
  private transient TiTableInfo bucketTable;
  private static final transient Logger logger = LoggerFactory.getLogger(TableStatistics.class);

  public TableStatistics(Supplier<ScheduledExecutorService> serviceSupplier,
                         int refreshPeriod,
                         TimeUnit periodUnit,
                         TiTableInfo tableInfo,
                         TiSession session) {
    this.tableInfo = tableInfo;
    if (session != null &&
        serviceSupplier != null) {
      serviceSupplier.get().scheduleAtFixedRate(this::refreshCache, refreshPeriod, refreshPeriod, periodUnit);
      Catalog catalog = session.getCatalog();
      metaTable = catalog.getTable("mysql", "stats_meta");
      histTable = catalog.getTable("mysql", "stats_histograms");
      bucketTable = catalog.getTable("mysql", "stats_buckets");
    }
  }

  private boolean isTablesReady() {
    return metaTable != null &&
        histTable != null &&
        bucketTable != null;
  }

  /**
   * Try to update table meta info according to the last updated version
   *
   * @return true, the meta info has been updated successfully, false otherwise
   */
  public boolean loadMetaInfo() {
    TiDAGRequest req =
        StatisticsHelper.buildMetaRequest(metaTable, tableInfo.getId(), snapshot.getVersion(), getVersion());
    Iterator<Row> metaResult = snapshot.tableRead(req);
    try {
      // if meta has been updated, proceed.
      if (metaResult.hasNext()) {
        Row row = metaResult.next();
        setCount(row.getLong(1));
        setModifyCount(row.getLong(2));
        setVersion(row.getLong(3));
        return true;
      }
    } catch (Exception e) {
      logger.warn(String.format("Update table [%s]'s meta status failed:", tableInfo.getName()), e);
    }
    return false;
  }

  private void refreshCache() {
    // Refresh logic
    if (!isTablesReady()) {
      return;
    }

    // 1.Refresh meta data
    boolean succeed = loadMetaInfo();
    if (succeed) {
      // 2. Update histogram
      loadHistogram();
    }
  }

  private void loadHistogram(boolean loadAll, List<Long> neededColIds) {
    TiDAGRequest req =
        StatisticsHelper.buildHistogramsRequest(histTable, tableInfo.getId(), snapshot.getVersion(), getVersion());
    Iterator<Row> rows = snapshot.tableRead(req);
    while (rows.hasNext()) {
      Row row = rows.next();
      Long histID = row.getLong(2);
      StatisticsHelper.StatisticsDTO dto =
          StatisticsHelper.extractStatisticsDTO(row, tableInfo, loadAll, neededColIds, histTable);
//      if (dto != null && dto.getVersion() > )
    }
  }

  public TableStatistics(TiTableInfo tableInfo) {
    this.tableInfo = tableInfo;
  }

  public TiTableInfo getTableInfo() {
    return tableInfo;
  }

  public Map<Long, ColumnStatistics> getColumnsHistMap() {
    return columnsHistMap;
  }

  public Map<Long, IndexStatistics> getIndexHistMap() {
    return indexHistMap;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long getModifyCount() {
    return modifyCount;
  }

  public void setModifyCount(long modifyCount) {
    this.modifyCount = modifyCount;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }
}
