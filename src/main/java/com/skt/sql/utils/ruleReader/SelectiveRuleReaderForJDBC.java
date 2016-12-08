package com.skt.sql.utils.ruleReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SelectiveRuleReaderForJDBC extends SelectiveRuleReader
{
  private final String url;
  private final String user;
  private final String PW;
  private final String query;

  public SelectiveRuleReaderForJDBC(
      String url,
      String user,
      String PW,
      String query
  )
  {
    this.url = url;
    this.user = user;
    this.PW = PW;
    this.query = query;
  }

  @Override
  List<List<String>> getRulesInternal()
  {
    DBI dbi = new DBI(url, user, PW);

    return dbi.withHandle(
        new HandleCallback<List<List<String>>>()
        {
          @Override
          public List<List<String>> withHandle(Handle handle) throws Exception
          {
            return handle.createQuery(query).map(
                new ResultSetMapper<List<String>>()
                {
                  @Override
                  public List<String> map(int index, ResultSet r, StatementContext ctx) throws SQLException
                  {
                    return Lists.newArrayList(
                        r.getString(1),
                        r.getString(2),
                        r.getString(3)
                    );
                  }
                }
            ).list();
          }
        }
    );
  }
}
