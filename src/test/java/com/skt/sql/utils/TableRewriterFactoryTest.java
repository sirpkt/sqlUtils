package com.skt.sql.utils;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class TableRewriterFactoryTest
{
  @Test
  public void simpleTest() throws SQLException
  {
    String[] sqls = {
        "SELECT * from (select * from \"테스트\".\"테이블\") xx",
        "SELECT * from \n abc.aaa",
        "SELECT \"aBc\".\"tEst\" from bbb",
        "SELECT * from abc.aaa t1 join bbb t2 on t1.a = t2.b",
        "SELECT * from abc.aaa t1 join (select * from def.test d1 join hjk.abc d2 on d1.a = d2.b) t2 on t1.a = t2.b",
        "SELECT * from bbb t1 join (select * from def.test) t2 on t1.a = t2.b",
        "SELECT abc.aaa from aBc.tEst",
        "SELECT abc.aaa from \"aBc\".\"tEst\"",
        "SELECT abc.aaa from aBc.\"tEst\"",
        "SELECT abc.aaa from \"aBc\".tEst"
    };

    TableRewriterFactory factory =
        new TableRewriterFactory(getClass().getClassLoader().getResource("rewrite_rules.csv").getFile());

    List<String> results = Lists.newArrayListWithCapacity(sqls.length);
    for (String sql: sqls) {
      TableRewriter rewriter = factory.getRewriter(sql);
      results.add(rewriter.rewrite());
    }
    String[] expected = {
        "SELECT * from (select * from test.table) xx",
        "SELECT * from \n (select *\n from xxx)",
        "SELECT \"aBc\".\"tEst\" from (select * from qqq)",
        "SELECT * from (select *\n from xxx) t1 join (select * from qqq) t2 on t1.a = t2.b",
        "SELECT * from (select *\n from xxx) t1 join (select * from ghi.testest d1 join hjk.abc d2 on d1.a = d2.b) t2 on t1.a = t2.b",
        "SELECT * from (select * from qqq) t1 join (select * from ghi.testest) t2 on t1.a = t2.b",
        "SELECT abc.aaa from abc.test",
        "SELECT abc.aaa from (select * from org.test)",
        "SELECT abc.aaa from abc.\"tEst\"",
        "SELECT abc.aaa from \"aBc\".test"
    };
    Assert.assertArrayEquals(expected, results.toArray(new String[sqls.length]));
  }

  @Test(expected = SQLException.class)
  public void testSQLError() throws Exception
  {
    String sql = "SELECT * FRO";

    TableRewriterFactory factory =
        new TableRewriterFactory(getClass().getClassLoader().getResource("rewrite_rules.csv").getFile());

    TableRewriter rewriter = factory.getRewriter(sql);
    Assert.assertNotNull(rewriter.rewrite());
  }

  @Test
  public void testHiveSQL() throws SQLException
  {
    String[] sqls = {
        "SELECT abc.aaa from `aBc`.`tEst`",
        "SELECT abc.aaa from aBc.`tEst`",
        "SELECT abc.aaa from `aBc`.tEst"
    };

    TableRewriterFactory factory =
        new TableRewriterFactory(getClass().getClassLoader().getResource("rewrite_rules.csv").getFile());

    List<String> results = Lists.newArrayListWithCapacity(sqls.length);
    for (String sql: sqls) {
      TableRewriter rewriter = factory.getRewriter(sql, '`');
      results.add(rewriter.rewrite());
      String tables = rewriter.summarizeTables();
    }

    Assert.assertEquals(sqls.length, results.size());
  }
}
