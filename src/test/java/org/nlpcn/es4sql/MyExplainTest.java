package org.nlpcn.es4sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.nlpcn.es4sql.TestsConstants.TEST_INDEX;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;

import com.google.common.io.Files;

public class MyExplainTest {
	
    @Test
    public void query1() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/orderby_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("select cust_dt,count(cust_code) from custom_daily_history/job group by cust_dt order by cust_dt desc,count(cust_code) desc limit  5000", TEST_INDEX));
        assertThat(result, equalTo(expectedOutput));
    }
    
    @Test
    public void query2() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/where_function_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("SELECT cust_code,birth from custom where birth>0 and modulus(birth,10000) > 1207 and  modulus(birth,10000) <1215", TEST_INDEX));
        System.out.println(result);
//        assertThat(result, equalTo(expectedOutput));
    }
    
    @Test
    public void query3() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/where_in_explain.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("select cust_code from   custom/job where  (cust_code in (184987575))", TEST_INDEX));
        assertThat(result, equalTo(expectedOutput));
    }
    
    @Test
    public void query4() throws IOException, SqlParseException, NoSuchMethodException, IllegalAccessException, SQLFeatureNotSupportedException, InvocationTargetException {
        String expectedOutput = Files.toString(new File("src/test/resources/expectedOutput/where_function_explain2.json"), StandardCharsets.UTF_8).replaceAll("\r","");
        String result = explain(String.format("SELECT cust_code FROM custom where (a>b) and (cust_type between 0 and 4 and priv_plac is null and non_trade is null) ", TEST_INDEX));
        assertThat(result, equalTo(expectedOutput));
    }
    
    private String explain(String sql) throws SQLFeatureNotSupportedException, SqlParseException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
		SearchDao searchDao = MainTestSuite.getSearchDao();
        SqlElasticRequestBuilder requestBuilder = searchDao.explain(sql).explain();
        return requestBuilder.explain();
	}
}
