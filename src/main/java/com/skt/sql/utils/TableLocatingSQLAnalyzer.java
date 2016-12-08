package com.skt.sql.utils;

import com.google.common.collect.Lists;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.Relation;
import org.apache.tajo.exception.SQLSyntaxError;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.parser.sql.SQLParser.Table_primaryContext;
import org.apache.tajo.util.Pair;

import java.sql.SQLException;
import java.util.List;

public class TableLocatingSQLAnalyzer extends SQLAnalyzer
{
  public static String[] getSqlLines(String sql)
  {
    return sql.split("\\r?\\n");
  }

  private static final char DEFAULT_QUOTE = '"';
  private final char inQuote, outQuote;
  private String[] sqlLines;
  private List<TableInfo> tables;

  public TableLocatingSQLAnalyzer(
      Character inQuote,
      Character outQuote
  )
  {
    this.inQuote = inQuote == null ? DEFAULT_QUOTE : inQuote;
    this.outQuote = outQuote == null ? DEFAULT_QUOTE : outQuote;
  }

  public TableLocatingSQLAnalyzer()
  {
    this(DEFAULT_QUOTE, DEFAULT_QUOTE);
  }

  public List<TableInfo> getTables(String sql) throws SQLException
  {
    sqlLines = getSqlLines(sql);
    tables = Lists.newArrayList();

    try {
      super.parse(sql);
    } catch (SQLSyntaxError e) {
      throw new SQLException(
          e.getMessage()
      );
    }

    return tables;
  }

  @Override
  public Expr visitTable_primary(Table_primaryContext ctx)
  {
    Expr expr = super.visitTable_primary(ctx);
    if (expr.getClass().equals(Relation.class))
    {
      Relation relation = (Relation)expr;
      Pair<String, String> schemaAndName = getSchemaAndName(relation.getName());
      tables.add(
          new TableInfo(
              tables.size() + 1,
              schemaAndName.getFirst(),
              schemaAndName.getSecond(),
              ctx.getStart().getLine(),
              ctx.getStart().getCharPositionInLine(),
              sqlLines,
              inQuote,
              outQuote
          )
      );
    }

    return expr;
  }

  private Pair<String, String> getSchemaAndName(String tableName)
  {
    String[] schemaAndName = tableName.split("\\.");
    if (schemaAndName.length == 1) {
      return new Pair<String, String>(null, schemaAndName[0]);
    } else {
      return new Pair<String, String>(schemaAndName[0], schemaAndName[1]);
    }
  }
}
