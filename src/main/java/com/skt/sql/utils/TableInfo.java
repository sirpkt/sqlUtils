package com.skt.sql.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.List;

public class TableInfo
{
  public static Comparator<TableInfo> COMPARATOR = new Comparator<TableInfo>()
  {
    public int compare(TableInfo info1, TableInfo info2)
    {
      return (info1.line == info2.line) ? (info1.posInLine - info2.posInLine) : (info1.line - info2.line);
    }
  };

  public static String summary(List<TableInfo> tables)
  {
    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter, true);
    writer.println("Detected tables (with position in SQL string");
    if (tables != null && !tables.isEmpty()) {
      for (TableInfo table: tables) {
        writer.println(table);
      }
    } else {
      writer.println("None");
    }

    return stringWriter.toString();
  }

  private final int num;
  private final String schema;
  private final String name;
  private final int line, posInLine;
  private final char outQuote;
  private final String fullName;

  private int tokenIndex;

  public TableInfo(
      int num,
      String schema,
      String name,
      int line,
      int posInLine,
      String[] sqlLines,
      char inQuote,
      char outQuote
  )
  {
    this.num = num;
    this.schema = schema;
    this.name = name;
    this.line = line;
    this.posInLine = posInLine;
    String lineToInspect = sqlLines[line - 1];
    this.outQuote = outQuote;

    if (schema != null) {
      if (lineToInspect.charAt(posInLine) == inQuote) {
        this.fullName = quoteIfNeeded(schema, true) + "."
            + quoteIfNeeded(name, lineToInspect.charAt(posInLine + schema.length() + 3) == inQuote);
      } else {
        this.fullName = quoteIfNeeded(schema, false) + "."
            + quoteIfNeeded(name, lineToInspect.charAt(posInLine + schema.length() + 1) == inQuote);
      }
    } else {
      this.fullName = quoteIfNeeded(name, (lineToInspect.charAt(posInLine) == inQuote));
    }

    this.tokenIndex = 0;
  }

  public String getFullName()
  {
    return fullName;
  }

  public String getSchema()
  {
    return schema;
  }

  public String getName()
  {
    return name;
  }

  public int getLine()
  {
    return line;
  }

  public int getPosInLine()
  {
    return posInLine;
  }

  public int getTokenIndex()
  {
    return tokenIndex;
  }

  public void setTokenIndex(int tokenIndex)
  {
    this.tokenIndex = tokenIndex;
  }

  @Override
  public String toString()
  {
    return String.format("%d - %s at line: %d, pos: %d", num, getFullName(), line, posInLine);
  }

  private String quoteIfNeeded(String target, boolean isNeeded)
  {
    return isNeeded ? (outQuote + target + outQuote) : target;
  }
}
