package com.skt.sql.utils.ruleReader;

import com.skt.sql.utils.TableRewritingRule;
import com.skt.sql.utils.ruleReader.SelectiveRuleReader;
import com.skt.sql.utils.ruleReader.SelectiveRuleReaderForCSV;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SelectiveRuleReaderForCSVTest
{
  @Test
  public void testSimpleRead()
  {
    String csvFile = getClass().getClassLoader().getResource("rewrite_rules.csv").getFile();
    SelectiveRuleReader ruleReader = new SelectiveRuleReaderForCSV(csvFile);

    List<TableRewritingRule> rules = ruleReader.getRules();

    Assert.assertEquals(5, rules.size());
  }
}
