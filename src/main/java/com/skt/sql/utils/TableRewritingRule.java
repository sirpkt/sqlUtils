package com.skt.sql.utils;

import com.skt.sql.utils.ruleReader.SelectiveRuleReader;
import com.skt.sql.utils.ruleReader.SelectiveRuleReaderForCSV;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class TableRewritingRule
{
  public static String getMatchedString(
      List<TableRewritingRule> rules,
      String schema,
      String name
  )
  {
    for (TableRewritingRule rule: rules) {
      if (rule.isMatch(schema, name)) {
        return rule.rewrote;
      }
    }

    return null;
  }

  public static String summary(List<TableRewritingRule> rules)
  {
    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter, true);
    writer.println("Converting rules");
    if (rules != null && !rules.isEmpty()) {
      for (TableRewritingRule rule: rules) {
        writer.println(rule);
      }
    } else {
      writer.println("None");
    }

    return stringWriter.toString();
  }

  public static List<TableRewritingRule> loadFromCSV(String fileName)
  {
    SelectiveRuleReader reader = new SelectiveRuleReaderForCSV(fileName);

    return reader.getRules();
  }

  public static List<TableRewritingRule> load(SelectiveRuleReader reader)
  {
    return reader.getRules();
  }

  private final int num;
  private final String schema;
  private final String name;
  private final String rewrote;

  public TableRewritingRule(
      int num,
      String schema,
      String name,
      String rewrote
  )
  {
    this.num = num;
    this.schema = schema;
    this.name = name;
    this.rewrote = rewrote;
  }

  public boolean isMatch(
      String schema,
      String name
  )
  {
    if (this.schema == null) {
      if (schema != null) {
        return false;
      }
    } else {
      if (schema == null || !this.schema.equals(schema)) {
        return false;
      }
    }

    return this.name.equals(name);
  }

  @Override
  public String toString()
  {
    return String.format("rule %d - %s => %s", num, (schema != null) ? (schema + "." + name) : name, rewrote);
  }
}
