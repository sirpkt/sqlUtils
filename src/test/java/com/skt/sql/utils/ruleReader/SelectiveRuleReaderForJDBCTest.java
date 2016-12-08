package com.skt.sql.utils.ruleReader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.skt.sql.utils.TableRewritingRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SelectiveRuleReaderForJDBCTest
{
  private final String tableName = "tableName";
  private final String schemaColumn = "col1";
  private final String tableColumn = "col2";
  private final String toColumn = "col3";

  private String uri;
  Handle handle;

  private List<List<String>> rules = ImmutableList.<List<String>>of(
      ImmutableList.of("aBc","tEst","(select * from org.test)"),
      ImmutableList.of("abc","aaa","(select * \nfrom xxx)"),
      Lists.newArrayList(null, "bbb", "(select * from qqq)"),
      ImmutableList.of("def","test","ghi.testest"),
      ImmutableList.of("테스트","테이블","test.table")
  );

  @Before
  public void setUp()
  {
    uri = "jdbc:derby:memory:druidTest" + UUID.randomUUID().toString().replace("-", "") + ";create=true";
    handle = new DBI(
        uri,
        null,
        null
    ).open();

    Assert.assertEquals(
        0,
        handle.createStatement(
            String.format(
                "CREATE TABLE %s (%s VARCHAR(64), %s VARCHAR(64), %s VARCHAR(64))",
                tableName,
                schemaColumn,
                tableColumn,
                toColumn
            )
        ).setQueryTimeout(1).execute()
    );
    handle.createStatement(String.format("TRUNCATE TABLE %s", tableName)).setQueryTimeout(1).execute();

    for (List<String> rule : rules) {
      insertValues(rule.get(0), rule.get(1), rule.get(2), handle);
    }
    handle.commit();
  }

  @Test
  public void testSimpleRead()
  {
    SelectiveRuleReader ruleReader = new SelectiveRuleReaderForJDBC(
        uri,
        null,
        null,
        String.format("select * from %s", tableName)
    );

    List<TableRewritingRule> convRules = ruleReader.getRules();
    Assert.assertEquals(rules.size(), convRules.size());
  }

  private void insertValues(final String schema, final String table, final String to, Handle handle)
  {
    final String query;
    handle.createStatement(
        String.format("DELETE FROM %s WHERE %s='%s' and %s='%s'", tableName, schemaColumn, schema, tableColumn, table)
    ).setQueryTimeout(1).execute();
    query = String.format(
        "INSERT INTO %s (%s, %s, %s) VALUES ('%s', '%s', '%s')",
        tableName,
        schemaColumn, tableColumn, toColumn,
        schema, table, to
    );
    Assert.assertEquals(1, handle.createStatement(query).setQueryTimeout(1).execute());
    handle.commit();
  }
}
