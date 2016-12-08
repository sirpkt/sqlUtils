package com.skt.sql.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.tajo.util.Pair;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TableRewriter
{
  private static final Character PARSER_QUOTE = '"';
  private final Character outQuote;
  private final String sql;
  private final List<TableRewritingRule> rules;

  private final TableLocatingSQLAnalyzer analyzer;
  private final String[] sqlLines;

  private List<TableInfo> tables = null;
  private String rewrote = null;

  public TableRewriter(
      String sql,
      Character outQuote,
      List<TableRewritingRule> rules
  )
  {
    this.sql = sql.replace(outQuote, PARSER_QUOTE);
    this.outQuote = outQuote;
    this.rules = rules;

    analyzer = new TableLocatingSQLAnalyzer(PARSER_QUOTE, outQuote);
    sqlLines = TableLocatingSQLAnalyzer.getSqlLines(sql);
  }

  public TableRewriter(
      String sql,
      List<TableRewritingRule> rules
  )
  {
    this(sql, PARSER_QUOTE, rules);
  }

  public List<TableInfo> getTables() throws SQLException
  {
    if (tables == null) {
      tables = analyzer.getTables(sql);
      Collections.sort(tables, TableInfo.COMPARATOR);
    }

    return tables;
  }

  public String summarizeTables() throws SQLException
  {
    return TableInfo.summary(getTables());
  }

  public String rewrite() throws SQLException
  {
    if (rewrote == null) {
      List<Pair<Integer, String>> tokens = tokenize();
      rewriteTokens(tokens);
      rewrote = tokensToString(tokens);
    }

    return rewrote;
  }

  private void rewriteTokens(List<Pair<Integer, String>> tokens) throws SQLException
  {
    for (TableInfo table: getTables()) {
      String rewrote = TableRewritingRule.getMatchedString(rules, table.getSchema(), table.getName());
      if (rewrote != null) {
        tokens.get(table.getTokenIndex()).setSecond(rewrote);
      }
    }
  }

  private List<Pair<Integer, String>> tokenize() throws SQLException
  {
    List<Pair<Integer, String>> tokens = Lists.newArrayList();

    int tokenizedLine = 0;

    List<TableInfo> tablesInLine = ImmutableList.of();
    Iterator<TableInfo> tableIterator = getTables().iterator();

    while (tableIterator.hasNext()) {
      TableInfo table = tableIterator.next();
      if (tokenizedLine != table.getLine()) {
        if (!tablesInLine.isEmpty()) {
          for(String localToken: tokenizeLocal(sqlLines[tokenizedLine - 1], tablesInLine, tokens.size())) {
            tokens.add(new Pair<Integer, String>(tokenizedLine, localToken));
          }
          tokenizedLine++;
        }
        for (; tokenizedLine < table.getLine(); tokenizedLine++) {
          if (tokenizedLine >=1) {
            tokens.add(new Pair<Integer, String>(tokenizedLine, sqlLines[tokenizedLine - 1]));
          }
        }

        tablesInLine = Lists.newArrayList(table);
      } else {
        tablesInLine.add(table);
      }
    }
    if (!tablesInLine.isEmpty()) {
      for(String localToken: tokenizeLocal(sqlLines[tokenizedLine - 1], tablesInLine, tokens.size())) {
        tokens.add(new Pair<Integer, String>(tokenizedLine, localToken));
      }
      tokenizedLine++;
    }

    for (; tokenizedLine <= sqlLines.length; tokenizedLine++) {
      if (tokenizedLine >=1) {
        tokens.add(new Pair<Integer, String>(tokenizedLine, sqlLines[tokenizedLine - 1]));
      }
    }

    return tokens;
  }

  private String tokensToString(List<Pair<Integer, String>> tokens)
  {
    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter, true);
    int index = 1;
    for (Pair<Integer, String> token: tokens) {
      if (token.getFirst() != index) {
        writer.println();
        index++;
      }
      writer.print(token.getSecond());
    }

    return stringWriter.toString();
  }

  private List<String> tokenizeLocal(String line, List<TableInfo> tables, int startIndex)
  {
    List<String> localTokens = Lists.newArrayList();
    int processedOffset = 0;
    for (TableInfo table: tables) {
      if (table.getPosInLine() != processedOffset) {
        localTokens.add(line.substring(processedOffset, table.getPosInLine()));
        startIndex++;
      }
      localTokens.add(table.getFullName());
      table.setTokenIndex(startIndex++);
      processedOffset = table.getPosInLine() + table.getFullName().length();
    }
    if (processedOffset < line.length()) {
      localTokens.add(line.substring(processedOffset));
    }
    return localTokens;
  }
}
