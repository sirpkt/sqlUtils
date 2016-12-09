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

  @Test
  public void testComplex() throws SQLException
  {
    String[] sqls = {
        "select city_id,\n" +
            "      city_name,\n" +
            "      mfact_cd,\n" +
            "      mfact_nm ,\n" +
            "      split(kpi_hdv, ',')[0] hdv_cei_value ,\n" +
            "      split(kpi_hdv, ',')[1] hdv_qoe1_value ,\n" +
            "      split(kpi_hdv, ',')[2] hdv_qoe2_value ,\n" +
            "      split(kpi_hdv, ',')[3] hdv_qoe1_kpi1 ,\n" +
            "      split(kpi_hdv, ',')[4] hdv_qoe1_kpi2 ,\n" +
            "      split(kpi_hdv, ',')[5] hdv_qoe1_kpi3 ,\n" +
            "      split(kpi_hdv, ',')[6] hdv_qoe1_qos1 \n" +
            " from (\n" +
            "       select city_id,\n" +
            "              city_name,\n" +
            "              mfact_cd,\n" +
            "              mfact_nm ,\n" +
            "              hdv_summary(hdv_calculated_et, hdv_bad_et, hdv_attempt_cnt, hdv_success_cnt, hdv_complete_cnt, hdv_drop_cnt , hdv_qoe1_kpi3_tmp, hdv_qoe2_value_tmp, hdv_user_cnt) kpi_hdv \n" +
            "         from (\n" +
            "               select a.city_id,\n" +
            "                      c.city_name,\n" +
            "                      b.mfact_cd,\n" +
            "                      b.mfact_nm,\n" +
            "                      sum(hdv_calculated_et) hdv_calculated_et ,\n" +
            "                      sum(hdv_bad_et) hdv_bad_et ,\n" +
            "                      sum(hdv_attempt_cnt) hdv_attempt_cnt ,\n" +
            "                      sum(hdv_success_cnt) hdv_success_cnt ,\n" +
            "                      sum(hdv_complete_cnt) hdv_complete_cnt ,\n" +
            "                      sum(hdv_drop_cnt) hdv_drop_cnt ,\n" +
            "                      sum(abs(hdv_qoe1_kpi3) * hdv_user_cnt) hdv_qoe1_kpi3_tmp ,\n" +
            "                      sum(abs(hdv_qoe2_value) * hdv_calculated_et) hdv_qoe2_value_tmp ,\n" +
            "                      sum(hdv_user_cnt) hdv_user_cnt \n" +
            "                 from o_samson_cem.cell_mdl_1d a inner join \n" +
            "                      o_samson_cm.cm_ukey_eqp_mdl b \n" +
            "                   on a.model_code = b.eqp_mdl_cd left outer join\n" +
            "                      o_samson_cm.cm_city c\n" +
            "                   on a.city_id = c.city_id\n" +
            "                where a.dt = '20160805'\n" +
            "                  and a.city_id != '0'\n" +
            "                group by a.city_id,\n" +
            "                         c.city_name, \n" +
            "                         b.mfact_cd,\n" +
            "                         b.mfact_nm\n" +
            "              ) a\n" +
            "      ) a\n",
            "SELECT  enb_id\n" +
            "       , split(kpi_hdv,\",\")[0] hdv_cei_value\n" +
            "       , split(kpi_hdv,\",\")[1] hdv_qoe1_value\n" +
            "       , split(kpi_hdv,\",\")[2] hdv_qoe2_value\n" +
            "       , split(kpi_hdv,\",\")[3] hdv_qoe1_kpi1\n" +
            "       , split(kpi_hdv,\",\")[4] hdv_qoe1_kpi2\n" +
            "       , split(kpi_hdv,\",\")[5] hdv_qoe1_kpi3\n" +
            "       , split(kpi_hdv,\",\")[6] hdv_qoe1_qos1\n" +
            "FROM\n" +
            "(\n" +
            "       SELECT  enb_id\n" +
            "               , hdv_summary (hdv_calculated_et, hdv_bad_et, hdv_attempt_cnt, hdv_success_cnt, hdv_complete_cnt, hdv_drop_cnt, hdv_qoe1_kpi3_tmp, hdv_qoe2_value_tmp, hdv_user_cnt) kpi_hdv\n" +
            "               \n" +
            "               -- test\n" +
            "       FROM\n" +
            "       (\n" +
            "           SELECT  enb_id\n" +
            "                           , sum(hdv_calculated_et) hdv_calculated_et\n" +
            "                           , sum(hdv_bad_et) hdv_bad_et\n" +
            "                           , sum(hdv_attempt_cnt) hdv_attempt_cnt\n" +
            "                           , sum(hdv_success_cnt) hdv_success_cnt\n" +
            "                           , sum(hdv_complete_cnt) hdv_complete_cnt\n" +
            "                           , sum(hdv_drop_cnt) hdv_drop_cnt\n" +
            "                           , sum(abs(hdv_qoe1_kpi3) * hdv_user_cnt) hdv_qoe1_kpi3_tmp\n" +
            "                           , sum(abs(hdv_qoe2_value) * hdv_calculated_et) hdv_qoe2_value_tmp\n" +
            "                           , sum(hdv_user_cnt) hdv_user_cnt\n" +
            "           FROM  o_samson_cem.enb_1d\n" +
            "           WHERE   dt>='20160701' AND dt<='20160730'\n" +
            "           GROUP BY enb_id\n" +
            "       ) a\n" +
            ") a\n",
            "SELECT  enb_id\n" +
            "       , split(kpi_wcdr,\",\")[0] wcdr_cei_value\n" +
            "       , split(kpi_wcdr,\",\")[1] wcdr_qoe1_value\n" +
            "       , split(kpi_wcdr,\",\")[2] wcdr_qoe2_value\n" +
            "       , split(kpi_wcdr,\",\")[3] wcdr_qoe1_kpi1\n" +
            "       , split(kpi_wcdr,\",\")[4] wcdr_qoe1_kpi2\n" +
            "       , split(kpi_wcdr,\",\")[5] wcdr_qoe1_qos1\n" +
            "\n" +
            "FROM\n" +
            "(\n" +
            "       SELECT  enb_id,\n" +
            "               csfb_summary(wcdr_calculated_et, wcdr_bad_et, wcdr_attempt_cnt, wcdr_success_cnt, wcdr_complete_cnt, wcdr_drop_cnt, wcdr_reattempt_cnt) kpi_wcdr\n" +
            "       FROM\n" +
            "       (\n" +
            "           SELECT  enb_id\n" +
            "                   , sum(wcdr_calculated_et) wcdr_calculated_et\n" +
            "                   , sum(wcdr_bad_et) wcdr_bad_et\n" +
            "                   , sum(wcdr_attempt_cnt) wcdr_attempt_cnt\n" +
            "                   , sum(wcdr_success_cnt) wcdr_success_cnt\n" +
            "                   , sum(wcdr_complete_cnt) wcdr_complete_cnt\n" +
            "                   , sum(wcdr_drop_cnt) wcdr_drop_cnt\n" +
            "                   , sum(wcdr_reattempt_cnt) wcdr_reattempt_cnt\n" +
            "           FROM  o_samson_cem.enb_1d\n" +
            "           WHERE   dt>='20160701' AND dt<='20160701'\n" +
            "           GROUP BY enb_id\n" +
            "       ) a\n" +
            ") a\n",
            "select city_id,\n" +
            "      city_name,\n" +
            "      mfact_cd,\n" +
            "      mfact_nm ,\n" +
            "      split(kpi_hdv, ',')[0] hdv_cei_value ,\n" +
            "      split(kpi_hdv, ',')[1] hdv_qoe1_value ,\n" +
            "      split(kpi_hdv, ',')[2] hdv_qoe2_value ,\n" +
            "      split(kpi_hdv, ',')[3] hdv_qoe1_kpi1 ,\n" +
            "      split(kpi_hdv, ',')[4] hdv_qoe1_kpi2 ,\n" +
            "      split(kpi_hdv, ',')[5] hdv_qoe1_kpi3 ,\n" +
            "      split(kpi_hdv, ',')[6] hdv_qoe1_qos1 \n" +
            " from (\n" +
            "       select city_id,\n" +
            "              city_name,\n" +
            "              mfact_cd,\n" +
            "              mfact_nm ,\n" +
            "              hdv_summary(hdv_calculated_et, hdv_bad_et, hdv_attempt_cnt, hdv_success_cnt, hdv_complete_cnt, hdv_drop_cnt , hdv_qoe1_kpi3_tmp, hdv_qoe2_value_tmp, hdv_user_cnt) kpi_hdv \n" +
            "         from (\n" +
            "               select a.city_id,\n" +
            "                      c.city_name,\n" +
            "                      b.mfact_cd,\n" +
            "                      b.mfact_nm,\n" +
            "                      sum(hdv_calculated_et) hdv_calculated_et ,\n" +
            "                      sum(hdv_bad_et) hdv_bad_et ,\n" +
            "                      sum(hdv_attempt_cnt) hdv_attempt_cnt ,\n" +
            "                      sum(hdv_success_cnt) hdv_success_cnt ,\n" +
            "                      sum(hdv_complete_cnt) hdv_complete_cnt ,\n" +
            "                      sum(hdv_drop_cnt) hdv_drop_cnt ,\n" +
            "                      sum(abs(hdv_qoe1_kpi3) * hdv_user_cnt) hdv_qoe1_kpi3_tmp ,\n" +
            "                      sum(abs(hdv_qoe2_value) * hdv_calculated_et) hdv_qoe2_value_tmp ,\n" +
            "                      sum(hdv_user_cnt) hdv_user_cnt \n" +
            "                 from o_samson_cem.cell_mdl_1d a inner join \n" +
            "                      o_samson_cm.cm_ukey_eqp_mdl b \n" +
            "                   on a.model_code = b.eqp_mdl_cd left outer join\n" +
            "                      o_samson_cm.cm_city c\n" +
            "                   on a.city_id = c.city_id\n" +
            "                where a.dt = '20160805'\n" +
            "                  and a.city_id != '0'\n" +
            "                group by a.city_id,\n" +
            "                         c.city_name, \n" +
            "                         b.mfact_cd,\n" +
            "                         b.mfact_nm\n" +
            "              ) a\n" +
            "      ) a\n",
            "select city_id,\n" +
            "      city_name,\n" +
            "      mfact_cd,\n" +
            "      mfact_nm ,\n" +
            "      split(kpi_lte, ',')[0] lte_cei_value ,\n" +
            "      split(kpi_lte, ',')[1] lte_qoe1 ,\n" +
            "      split(kpi_lte, ',')[2] lte_qoe1_qos1 ,\n" +
            "      split(kpi_lte, ',')[3] lte_qoe2 ,\n" +
            "      split(kpi_lte, ',')[4] lte_qoe2_qos1 ,\n" +
            "      split(kpi_lte, ',')[5] lte_qoe2_qos2 ,\n" +
            "      split(kpi_lte, ',')[6] lte_qoe2_qos3 ,\n" +
            "      split(kpi_lte, ',')[7] lte_qoe2_qos4 ,\n" +
            "      split(kpi_lte, ',')[8] lte_qoe3 ,\n" +
            "      split(kpi_lte, ',')[9] lte_qoe3_qos1 ,\n" +
            "      split(kpi_lte, ',')[10] lte_qoe3_qos2 ,\n" +
            "      split(kpi_lte, ',')[11] lte_qoe3_qos3 ,\n" +
            "      split(kpi_lte, ',')[12] lte_qoe3_qos4 ,\n" +
            "      split(kpi_lte, ',')[13] lte_qoe3_qos5 ,\n" +
            "      split(kpi_lte, ',')[14] lte_qoe1_kpi1 ,\n" +
            "      split(kpi_lte, ',')[15] lte_qoe1_kpi2 ,\n" +
            "      split(kpi_lte, ',')[16] lte_qoe1_kpi3 ,\n" +
            "      split(kpi_lte, ',')[17] lte_qoe1_kpi4 ,\n" +
            "      split(kpi_lte, ',')[18] lte_qoe1_kpi5 \n" +
            " from (\n" +
            "       select city_id,\n" +
            "              city_name,\n" +
            "              mfact_cd,\n" +
            "              mfact_nm ,\n" +
            "              data_summary(lte_et, lte_calculated_et, lte_attempt_cnt, lte_success_cnt, lte_data_attempt_cnt, lte_data_success_cnt , lte_ims_attempt_cnt, lte_ims_success_cnt, lte_dns_attempt_cnt, lte_dns_success_cnt, lte_drop_cnt , lte_bad_et, lte_qoe1_kpi5_tmp, qoe2, qoe3, lte_user_cnt) kpi_lte \n" +
            "         from (\n" +
            "               select a.city_id,\n" +
            "                      c.city_name,\n" +
            "                      b.mfact_cd,\n" +
            "                      b.mfact_nm ,\n" +
            "                      sum(lte_et) lte_et ,\n" +
            "                      sum(lte_calculated_et) lte_calculated_et ,\n" +
            "                      sum(lte_attempt_cnt) lte_attempt_cnt ,\n" +
            "                      sum(lte_success_cnt) lte_success_cnt ,\n" +
            "                      sum(lte_data_attempt_cnt) lte_data_attempt_cnt ,\n" +
            "                      sum(lte_data_success_cnt) lte_data_success_cnt ,\n" +
            "                      sum(lte_ims_attempt_cnt) lte_ims_attempt_cnt ,\n" +
            "                      sum(lte_ims_success_cnt) lte_ims_success_cnt ,\n" +
            "                      sum(lte_dns_attempt_cnt) lte_dns_attempt_cnt ,\n" +
            "                      sum(lte_dns_success_cnt) lte_dns_success_cnt ,\n" +
            "                      sum(lte_drop_cnt) lte_drop_cnt ,\n" +
            "                      sum(lte_bad_et) lte_bad_et ,\n" +
            "                      sum(abs(lte_qoe1_kpi5) * lte_user_cnt) lte_qoe1_kpi5_tmp ,\n" +
            "                      collect_list(case when qoe2 = '' then NULL else qoe2 end) qoe2 ,\n" +
            "                      collect_list(case when qoe3 = '' then NULL else qoe3 end) qoe3 ,\n" +
            "                      sum(lte_user_cnt) lte_user_cnt \n" +
            "                 from o_samson_cem.cell_mdl_1d a inner join \n" +
            "                      o_samson_cm.cm_ukey_eqp_mdl b \n" +
            "                   on a.model_code = b.eqp_mdl_cd left outer join \n" +
            "                      o_samson_cm.cm_city c \n" +
            "                   on a.city_id = c.city_id \n" +
            "                where a.dt = '20160805'\n" +
            "                  and a.city_id != '0'\n" +
            "                group by a.city_id,\n" +
            "                      c.city_name,\n" +
            "                      b.mfact_cd,\n" +
            "                      b.mfact_nm\n" +
            "              ) a\n" +
            "      ) a  limit 1\n",
            "select city_id,\n" +
            "      city_name,\n" +
            "      mfact_cd,\n" +
            "      mfact_nm ,\n" +
            "      split(kpi_wcdr, ',')[0] wcdr_cei_value ,\n" +
            "      split(kpi_wcdr, ',')[1] wcdr_qoe1_value ,\n" +
            "      split(kpi_wcdr, ',')[2] wcdr_qoe2_value ,\n" +
            "      split(kpi_wcdr, ',')[3] wcdr_qoe1_kpi1 ,\n" +
            "      split(kpi_wcdr, ',')[4] wcdr_qoe1_kpi2 ,\n" +
            "      split(kpi_wcdr, ',')[5] wcdr_qoe1_qos1 \n" +
            " from (\n" +
            "       select city_id,\n" +
            "              city_name,\n" +
            "              mfact_cd,\n" +
            "              mfact_nm ,\n" +
            "              csfb_summary(wcdr_calculated_et, wcdr_bad_et, wcdr_attempt_cnt, wcdr_success_cnt, wcdr_complete_cnt, wcdr_drop_cnt, wcdr_reattempt_cnt) kpi_wcdr \n" +
            "         from (\n" +
            "               select a.city_id,\n" +
            "                      c.city_name,\n" +
            "                      b.mfact_cd,\n" +
            "                      b.mfact_nm ,\n" +
            "                      sum(wcdr_calculated_et) wcdr_calculated_et ,\n" +
            "                      sum(wcdr_bad_et) wcdr_bad_et ,\n" +
            "                      sum(wcdr_attempt_cnt) wcdr_attempt_cnt ,\n" +
            "                      sum(wcdr_success_cnt) wcdr_success_cnt ,\n" +
            "                      sum(wcdr_complete_cnt) wcdr_complete_cnt ,\n" +
            "                      sum(wcdr_drop_cnt) wcdr_drop_cnt ,\n" +
            "                      sum(wcdr_reattempt_cnt) wcdr_reattempt_cnt \n" +
            "                 from o_samson_cem.cell_mdl_1d a inner join \n" +
            "                      o_samson_cm.cm_ukey_eqp_mdl b \n" +
            "                   on a.model_code = b.eqp_mdl_cd left outer join \n" +
            "                      o_samson_cm.cm_city c \n" +
            "                   on a.city_id = c.city_id \n" +
            "                where a.dt = '20160805'\n" +
            "                  and a.city_id != '0'\n" +
            "                group by a.city_id,\n" +
            "                      c.city_name,\n" +
            "                      b.mfact_cd,\n" +
            "                      b.mfact_nm\n" +
            "              ) a\n" +
            "      ) a"
    };

    TableRewriterFactory factory =
        new TableRewriterFactory(getClass().getClassLoader().getResource("rewrite_rules.csv").getFile());

    List<String> results = Lists.newArrayListWithCapacity(sqls.length);
    for (String sql: sqls) {
      TableRewriter rewriter = factory.getRewriter(sql);
      results.add(rewriter.rewrite());
    }

    Assert.assertEquals(sqls.length, results.size());
  }
}
