package com.skt.sql.utils.ruleReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class SelectiveRuleReaderForCSV extends SelectiveRuleReader
{
  private static List<String> DEFAULT_COLUMNS = ImmutableList.of("schema", "table", "to");
  private final List<String> columns;
  private final String schemaColumn;
  private final String tableColumn;
  private final String rewroteColumn;
  private final String fileName;
  private final int[] columnIndex;

  public SelectiveRuleReaderForCSV(
      String fileName,
      List<String> columns,
      String schemaColumn,
      String tableColumn,
      String rewroteColumn
  )
  {
    this.columns = Preconditions.checkNotNull(columns);
    this.schemaColumn = Preconditions.checkNotNull(schemaColumn);
    this.tableColumn = Preconditions.checkNotNull(tableColumn);
    this.rewroteColumn = Preconditions.checkNotNull(rewroteColumn);

    Preconditions.checkArgument(columns.contains(schemaColumn), "schema column %s not exists in columns", schemaColumn);
    Preconditions.checkArgument(columns.contains(tableColumn), "schema column %s not exists in columns", tableColumn);
    Preconditions.checkArgument(columns.contains(rewroteColumn), "schema column %s not exists in columns", rewroteColumn);

    this.fileName = fileName;
    columnIndex = new int[3];

    columnIndex[0] = columns.indexOf(schemaColumn);
    columnIndex[1] = columns.indexOf(tableColumn);
    columnIndex[2] = columns.indexOf(rewroteColumn);
  }

  public SelectiveRuleReaderForCSV(
      String fileName
  )
  {
    this(
        fileName,
        DEFAULT_COLUMNS,
        DEFAULT_COLUMNS.get(0),
        DEFAULT_COLUMNS.get(1),
        DEFAULT_COLUMNS.get(2)
    );
  }

  @Override
  List<List<String>> getRulesInternal()
  {
    List<List<String>> rules = Lists.newArrayList();
    try {
      CSVReader reader = new CSVReader(new FileReader(fileName));

      for (String[] line : reader) {
        Preconditions.checkArgument(line.length == columns.size(),
            "Malformed CSV file - expected %d columns but %d columns", columns.size(), line.length);
        rules.add(Lists.newArrayList(
            emptyToNull(line[columnIndex[0]]),
            line[columnIndex[1]],
            line[columnIndex[2]]
        ));
      }

      return rules;
    } catch (IOException e) {
      return null;
    }
  }

  private String emptyToNull(String schema)
  {
    return schema.equals("") ? null: schema;
  }
}
