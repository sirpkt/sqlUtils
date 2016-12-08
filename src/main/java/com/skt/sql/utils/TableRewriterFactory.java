package com.skt.sql.utils;

import com.skt.sql.utils.ruleReader.SelectiveRuleReader;

import java.util.List;

public class TableRewriterFactory
{
  private final List<TableRewritingRule> rules;

  public TableRewriterFactory(
      SelectiveRuleReader ruleReader
  )
  {
    this.rules = TableRewritingRule.load(ruleReader);
  }

  public TableRewriterFactory(
      String csvFile
  )
  {
    this.rules = TableRewritingRule.loadFromCSV(csvFile);
  }

  public TableRewriter getRewriter(
    String sql
  )
  {
    return new TableRewriter(sql, rules);
  }

  public TableRewriter getRewriter(
      String sql,
      Character quote
  )
  {
    return new TableRewriter(sql, quote, rules);
  }

  public String summarizeRules()
  {
    return TableRewritingRule.summary(rules);
  }
}
