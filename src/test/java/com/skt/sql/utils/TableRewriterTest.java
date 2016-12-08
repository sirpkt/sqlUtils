package com.skt.sql.utils;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class TableRewriterTest
{
  @Test
  public void testTableLocatingSQLAnalyzer() throws SQLException
  {
    TableLocatingSQLAnalyzer analyzer = new TableLocatingSQLAnalyzer();
    List<TableInfo> tables = analyzer.getTables("select * from test.abc t1 join \"tEst\".\"dEf\" t2 on t1.a = t2.b");
    List<TableInfo> tables2 = analyzer.getTables("select * from \"abC\".def t1 \njoin DEF.\"xXx\" t2 on t1.a = t2.b");

    Assert.assertEquals(1, tables.get(0).getLine());
    Assert.assertEquals(14, tables.get(0).getPosInLine());
    Assert.assertEquals(1, tables.get(1).getLine());
    Assert.assertEquals(31, tables.get(1).getPosInLine());
    Assert.assertEquals(1, tables2.get(0).getLine());
    Assert.assertEquals(14, tables2.get(0).getPosInLine());
    Assert.assertEquals(2, tables2.get(1).getLine());
    Assert.assertEquals(5, tables2.get(1).getPosInLine());
  }

  @Test
  public void testTableRewriter() throws SQLException
  {
    String sql = "select * from \"abC\".def t1 \njoin DEF.\"xXx\" t2 on t1.a = t2.b";
    List<TableRewritingRule> rules = ImmutableList.of(
        new TableRewritingRule(1, "abC", "def", "yyy.zzz")
    );
    TableRewriter rewriter = new TableRewriter(sql, rules);
    String rewrote = rewriter.rewrite();
    String expected = "select * from yyy.zzz t1 \njoin def.\"xXx\" t2 on t1.a = t2.b";
    Assert.assertEquals(expected, rewrote);
  }
}
