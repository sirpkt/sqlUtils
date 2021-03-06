package com.skt.sql.utils;

import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.Relation;
import org.apache.tajo.exception.SQLSyntaxError;
import org.apache.tajo.parser.sql.SQLLexer;
import org.apache.tajo.parser.sql.SQLParser;
import org.apache.tajo.parser.sql.SQLParser.Table_primaryContext;
import org.apache.tajo.parser.sql.SQLParserBaseVisitor;
import org.apache.tajo.util.Pair;

import java.sql.SQLException;
import java.util.List;

public class TableLocatingSQLAnalyzer extends SQLParserBaseVisitor<Expr>
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
      getExpr(sql);
    } catch (SQLSyntaxError e) {
      throw new SQLException(
          e.getMessage()
      );
    }

    return tables;
  }

  private Expr getExpr(String sql) throws SQLSyntaxError
  {
    final ANTLRInputStream input = new ANTLRInputStream(sql);
    final SQLLexer lexer = new SQLLexer(input);
    lexer.removeErrorListeners();
    lexer.addErrorListener(new SQLErrorListener());

    final CommonTokenStream tokens = new CommonTokenStream(lexer);

    final SQLParser parser = new SQLParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new SQLErrorListener());
    parser.setBuildParseTree(true);

    SQLParser.SqlContext context;
    try {
      context = parser.sql();
    } catch (RuntimeException e) {
      throw new SQLSyntaxError(e.getMessage());
    }

    return visitSql(context);
  }

  @Override
  public Expr visitTable_primary(Table_primaryContext ctx)
  {
    if (ctx.table_or_query_name() != null) {
      Pair<String, String> schemaAndName = getSchemaAndName(ctx.table_or_query_name().getText());
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
    } else if (ctx.derived_table() != null) {
      visit(ctx.derived_table().table_subquery());
    }

    return null;
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
