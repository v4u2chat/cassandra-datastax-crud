package com.sw.cassandra.demo.dao;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.sw.cassandra.demo.init.DataSource;


public class TimeSeriesAggDAO {
	
	private Session session;

	public void setSession(Session session) {
		this.session = session;
	}

	public void querySchema() 
	{
		String cqlString = "SELECT period_id,ts_metric FROM rakesh.sw_worksheet_timeseries_agg WHERE worksheet_pk='9d9062670a0400e211b41e95afa24b2b' and parent_name='PARENT00020' and tsname='Commercial Demand Plan (Rev)';";
		System.out.println(cqlString);
		
		ResultSet results = session.execute(cqlString);
		System.out.println(String.format("%-30s\t%-20s\n%s", "period_id", "ts_metric", "-------------------------------+-----------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-20s", row.getInt("period_id"), row.getDouble("ts_metric")));
		}
		System.out.println();
	}
	
	
	public void readAll() 
	{
		String cqlString = "SELECT * FROM rakesh.sw_worksheet_timeseries_agg;";
		System.out.println(cqlString);
		
		ResultSet results = session.execute(cqlString);
		System.out.println(String.format("%-35s\t%-20s\t%-50s\t%-20s\t%-20s\n%s", "worksheet_pk","parent_name","tsname","period_id", "ts_metric", "------------------------------------+-----------------------+---------------------------------------------------+-----------------------+-----------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-35s\t%-20s\t%-50s\t%-20s\t%-20s", row.getString("worksheet_pk"), row.getString("parent_name"),row.getString("tsname"),row.getInt("period_id"),row.getDouble("ts_metric")));
		}
		System.out.println();
	}

	public static void main(String[] args)
	{
		DataSource dataSource = new DataSource();
		dataSource.connect("localhost");
		
		
		TimeSeriesAggDAO timeSeriesAggDAO = new TimeSeriesAggDAO();
		timeSeriesAggDAO.setSession(dataSource.getSession());
		
		//timeSeriesAggDAO.querySchema();
		
		timeSeriesAggDAO.readAll();
	}

}
