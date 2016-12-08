package com.skt.sql.utils.ruleReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.skt.sql.utils.TableRewritingRule;

import java.util.List;

public abstract class SelectiveRuleReader
{
  public List<TableRewritingRule> getRules()
  {
    List<List<String>> rulesInternal = getRulesInternal();
    List<TableRewritingRule> rules = Lists.newArrayListWithCapacity(rulesInternal.size());

    for (List<String> rule: rulesInternal) {
      Preconditions.checkArgument(rule.size() == 3);
      rules.add(
          new TableRewritingRule(
              rules.size() + 1,
              rule.get(0),
              rule.get(1),
              rule.get(2)
          )
      );
    }

    return rules;
  }

  abstract List<List<String>> getRulesInternal();
}
