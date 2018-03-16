package com.pingcap.tikv.statistics;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.CMSketchRow;
import com.pingcap.tikv.expression.ByItem;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.MySQLType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.pingcap.tikv.meta.TiDAGRequest.PushDownType.NORMAL;

public class StatisticsHelper {
  private static final Logger logger = LoggerFactory.getLogger(StatisticsHelper.class);

  public static class StatisticsDTO {
    private Long colId;
    private Integer isIndex;
    private Long distinct;
    private Long version;
    private Long nullCount;
    private DataType dataType;
    private byte[] rawCMSketch;
    private TiIndexInfo idxInfo;
    private TiColumnInfo colInfo;

    StatisticsDTO(Long colId, Integer isIndex, Long distinct, Long version, Long nullCount, DataType dataType, byte[] rawCMSketch, TiIndexInfo idxInfo, TiColumnInfo colInfo) {
      this.colId = colId;
      this.isIndex = isIndex;
      this.distinct = distinct;
      this.version = version;
      this.nullCount = nullCount;
      this.dataType = dataType;
      this.rawCMSketch = rawCMSketch;
      this.idxInfo = idxInfo;
      this.colInfo = colInfo;
    }

    public Long getColId() {
      return colId;
    }

    public void setColId(Long colId) {
      this.colId = colId;
    }

    public Integer getIsIndex() {
      return isIndex;
    }

    public void setIsIndex(Integer isIndex) {
      this.isIndex = isIndex;
    }

    public Long getDistinct() {
      return distinct;
    }

    public void setDistinct(Long distinct) {
      this.distinct = distinct;
    }

    public Long getVersion() {
      return version;
    }

    public void setVersion(Long version) {
      this.version = version;
    }

    public Long getNullCount() {
      return nullCount;
    }

    public void setNullCount(Long nullCount) {
      this.nullCount = nullCount;
    }

    public DataType getDataType() {
      return dataType;
    }

    public void setDataType(DataType dataType) {
      this.dataType = dataType;
    }

    public byte[] getRawCMSketch() {
      return rawCMSketch;
    }

    public void setRawCMSketch(byte[] rawCMSketch) {
      this.rawCMSketch = rawCMSketch;
    }

    public TiIndexInfo getIdxInfo() {
      return idxInfo;
    }

    public void setIdxInfo(TiIndexInfo idxInfo) {
      this.idxInfo = idxInfo;
    }

    public TiColumnInfo getColInfo() {
      return colInfo;
    }

    public void setColInfo(TiColumnInfo colInfo) {
      this.colInfo = colInfo;
    }
  }

  public static class StatisticsResult {
    private Long histId;
    private Histogram histogram;
    private CMSketch cMSketch;
    private TiIndexInfo idxInfo;
    private TiColumnInfo colInfo;

    StatisticsResult(Long histId, Histogram histogram, CMSketch cMSketch, TiIndexInfo idxInfo, TiColumnInfo colInfo) {
      this.histId = histId;
      this.histogram = histogram;
      this.cMSketch = cMSketch;
      this.idxInfo = idxInfo;
      this.colInfo = colInfo;
    }

    public boolean hasIdxInfo() {
      return idxInfo != null;
    }

    public boolean hasColInfo() {
      return colInfo != null;
    }

    public Long getHistId() {
      return histId;
    }

    public void setHistId(Long histId) {
      this.histId = histId;
    }

    public Histogram getHistogram() {
      return histogram;
    }

    public void setHistogram(Histogram histogram) {
      this.histogram = histogram;
    }

    public CMSketch getcMSketch() {
      return cMSketch;
    }

    public void setcMSketch(CMSketch cMSketch) {
      this.cMSketch = cMSketch;
    }

    public TiIndexInfo getIdxInfo() {
      return idxInfo;
    }

    public void setIdxInfo(TiIndexInfo idxInfo) {
      this.idxInfo = idxInfo;
    }

    public TiColumnInfo getColInfo() {
      return colInfo;
    }

    public void setColInfo(TiColumnInfo colInfo) {
      this.colInfo = colInfo;
    }
  }

  private static final String[] metaRequiredCols = new String[]{
      "table_id",
      "count",
      "modify_count",
      "version"
  };

  private static final String[] histRequiredCols = new String[]{
      "table_id",
      "is_index",
      "hist_id",
      "distinct_count",
      "version",
      "null_count",
      "cm_sketch"
  };

  private static final String[] bucketRequiredCols = new String[]{
      "count",
      "repeats",
      "lower_bound",
      "upper_bound",
      "bucket_id",
      "table_id",
      "is_index",
      "hist_id"
  };

  private static boolean checkColExists(TiTableInfo table, String column) {
    return table.getColumns().stream().anyMatch(t -> t.matchName(column));
  }

  public static StatisticsDTO extractStatisticsDTO(Row row,
                                                   TiTableInfo table,
                                                   Boolean loadAll,
                                                   List<Long> neededColIds,
                                                   TiTableInfo histTable) {
    if (row.fieldCount() < 6) return null;
    Boolean isIndex = row.getLong(1) > 0;
    Long histID = row.getLong(2);
    Long distinct = row.getLong(3);
    Long histVer = row.getLong(4);
    Long nullCount = row.getLong(5);
    byte[] cMSketch = checkColExists(histTable, "cm_sketch") ? row.getBytes(6) : null;
    Stream<TiIndexInfo> indexInfos =
        table.getIndices().stream().filter(i -> i.getId() == histID);
    Stream<TiColumnInfo> colInfos =
        table.getColumns().stream().filter(t -> t.getId() == histID);
    boolean needed = true;

    // we should only query those columns that user specified before
    if (!loadAll && !neededColIds.contains(histID)) needed = false;

    int indexFlag = 1;
    DataType dataType = DataTypeFactory.of(MySQLType.TypeBlob);
    // Columns info found
    if (!isIndex && colInfos.findFirst().isPresent()) {
      indexFlag = 0;
      dataType = colInfos.findFirst().get().getType();
    } else if (!isIndex || !indexInfos.findFirst().isPresent()) {
      logger.error(
          "We cannot find histogram id $histID in table info " + table.getName() + " now. It may be deleted."
      );
      needed = false;
    }

    if (needed) {
      return new StatisticsDTO(
          histID,
          indexFlag,
          distinct,
          histVer,
          nullCount,
          dataType,
          cMSketch,
          indexInfos.findFirst().orElse(null),
          colInfos.findFirst().orElse(null)
      );
    } else {
      return null;
    }
  }

  public static StatisticsResult extractStatisticResult(Long histId,
                                                        Iterator<Row> rows,
                                                        List<StatisticsDTO> requests) {
    Stream<StatisticsDTO> matches = requests.stream().filter(r -> r.colId.equals(histId));
    if (matches.findFirst().isPresent()) {
      StatisticsDTO matched = matches.findFirst().get();
      Long totalCount = 0L;
      List<Bucket> buckets = new ArrayList<>();
      while (rows.hasNext()) {
        Row row = rows.next();
        Long count = row.getLong(0);
        Long repeats = row.getLong(1);
        // all bounds are stored as blob in bucketTable currently, decode using blob type
        Key lowerBound =
            TypedKey.toTypedKey(row.getBytes(2), DataTypeFactory.of(MySQLType.TypeBlob));
        Key upperBound =
            TypedKey.toTypedKey(row.getBytes(3), DataTypeFactory.of(MySQLType.TypeBlob));
        totalCount += count;
        buckets.add(new Bucket(totalCount, repeats, lowerBound, upperBound));
      }
      // create histogram for column `colId`
      Histogram histogram = Histogram
          .newBuilder()
          .setId(matched.colId)
          .setNDV(matched.distinct)
          .setNullCount(matched.nullCount)
          .setLastUpdateVersion(matched.version)
          .setBuckets(buckets)
          .build();
      // parse CMSketch
      byte[] rawData = matched.rawCMSketch;
      CMSketch cMSketch = null;
      if (rawData != null && rawData.length > 0) {
        try {
          com.pingcap.tidb.tipb.CMSketch sketch =
              com.pingcap.tidb.tipb.CMSketch.parseFrom(rawData);
          CMSketch result =
              CMSketch.newCMSketch(sketch.getRowsCount(), sketch.getRows(0).getCountersCount());
          for (int i = 0; i < sketch.getRowsCount(); i++) {
            CMSketchRow row = sketch.getRows(i);
            result.setCount(0);
            for (int j = 0; j < row.getCountersCount(); j++) {
              int counter = row.getCounters(j);
              result.getTable()[i][j] = counter;
              result.setCount(result.getCount() + counter);
            }
          }
          cMSketch = result;
        } catch (InvalidProtocolBufferException ignored) {
        }
      }
      return new StatisticsResult(histId, histogram, cMSketch, matched.idxInfo, matched.colInfo);
    } else {
      return null;
    }
  }

  public static TiDAGRequest buildHistogramsRequest(TiTableInfo histTable,
                                                    Long targetTblId,
                                                    Long startTs) {
    return buildHistogramsRequest(histTable, targetTblId, startTs, null);
  }

  public static TiDAGRequest buildHistogramsRequest(TiTableInfo histTable,
                                                    Long targetTblId,
                                                    Long startTs,
                                                    Long version) {
    TiDAGRequest.Builder builder = TiDAGRequest.Builder.newBuilder();
    if (version != null) {
      builder.addFilter(
          ComparisonBinaryExpression
              .greaterThan(ColumnRef.create("version"), Constant.create(version))
      );
    }
    return builder
        .setFullTableScan(histTable)
        .addFilter(
            ComparisonBinaryExpression
                .equal(ColumnRef.create("table_id"), Constant.create(targetTblId))
        )
        .addRequiredCols(
            Arrays.stream(histRequiredCols)
                .filter(c -> checkColExists(histTable, c))
                .collect(Collectors.toList())
        )
        .setStartTs(startTs)
        .build(NORMAL);
  }

  public static TiDAGRequest buildMetaRequest(TiTableInfo metaTable,
                                              Long targetTblId,
                                              Long startTs) {
    return buildMetaRequest(metaTable, targetTblId, startTs, null);
  }

  public static TiDAGRequest buildMetaRequest(TiTableInfo metaTable,
                                              Long targetTblId,
                                              Long startTs,
                                              Long version) {
    TiDAGRequest.Builder builder = TiDAGRequest.Builder.newBuilder();
    if (version != null) {
      builder.addFilter(
          ComparisonBinaryExpression
              .greaterThan(ColumnRef.create("version"), Constant.create(version))
      );
    }
    return builder
        .setFullTableScan(metaTable)
        .addFilter(
            ComparisonBinaryExpression
                .equal(ColumnRef.create("table_id"), Constant.create(targetTblId))
        )
        .addOrderBy(ByItem.create(ColumnRef.create("version"), false))
        .addRequiredCols(
            Arrays.stream(metaRequiredCols)
                .filter(c -> checkColExists(metaTable, c))
                .collect(Collectors.toList())
        )
        .setStartTs(startTs)
        .build(NORMAL);
  }

  public static TiDAGRequest buildBucketRequest(TiTableInfo bucketTable,
                                                Long targetTblId,
                                                Long startTs) {
    return TiDAGRequest.Builder
        .newBuilder()
        .setFullTableScan(bucketTable)
        .addFilter(
            ComparisonBinaryExpression
                .equal(ColumnRef.create("table_id"), Constant.create(targetTblId))
        )
        .setLimit(Integer.MAX_VALUE)
        .addOrderBy(ByItem.create(ColumnRef.create("bucket_id"), false))
        .addRequiredCols(
            Arrays.stream(bucketRequiredCols)
                .filter(c -> checkColExists(bucketTable, c))
                .collect(Collectors.toList())
        )
        .setStartTs(startTs)
        .build(NORMAL);
  }
}
