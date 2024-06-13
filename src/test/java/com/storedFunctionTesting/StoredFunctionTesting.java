package com.storedFunctionTesting;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class StoredFunctionTesting {
	
	Connection connection=null;
	Statement statement=null;
	ResultSet result;
	ResultSet resultSet1;
	ResultSet resultSet2;
	CallableStatement callableStatement;
	
	@BeforeClass
	void setUp() throws SQLException {
		
		connection=DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","root");
		System.out.println("Connection started");
	}
	
	@AfterClass
	void tearDown() throws SQLException {
		
		connection.close();
		System.out.println("Connection closed");
	}
	
	@Test(priority = 1)
	void test_storedFunctionExistsTC01() throws SQLException {
		
		statement=connection.createStatement();
		String s="show function status where db='classicmodels'";
		String s1="show function status where name='customerlevel'";
		result=statement.executeQuery(s1);
		result.next();
		String name=result.getString("Name");
		System.out.println(name);
		
		Assert.assertEquals(name,"CustomerLevel");
		
		
	}
	
	@Test(priority = 2)
	void storedFunctionTestWithSQLStatementTC02() throws SQLException
	{
		statement=connection.createStatement();
		String s="select customerName,customerlevel(creditlimit) from customers";
		resultSet1=statement.executeQuery(s);
		
		statement=connection.createStatement();
		String s1="select customername ,case when creditlimit>50000 then 'PLATINUM' when creditlimit>=10000 and creditlimit<=50000 then 'GOLD' when creditlimit<10000 then 'SILVER' end as customerlevel from customers";
		resultSet2=statement.executeQuery(s1);
		
		boolean compareResult=compareResultSets(resultSet1, resultSet2);
		Assert.assertEquals(compareResult, true, "Exact match");
		System.out.println("Exact match");
		
		
		
		
	}
	
	@Test(priority = 3)
	public void callmethodTC03() throws SQLException {
		GetCustomerLevelStoredProcedurewithStoredFunctionTC03(125);
		GetCustomerLevelStoredProcedurewithStoredFunctionTC03(121);
		GetCustomerLevelStoredProcedurewithStoredFunctionTC03(103);
	}
	
	public void GetCustomerLevelStoredProcedurewithStoredFunctionTC03(int data) throws SQLException {
		
		callableStatement=connection.prepareCall("{call GetCustomerLevel(?,?)}");
		callableStatement.setInt(1, data);  //112, 202,186
		//setting output parameters
		callableStatement.registerOutParameter(2, Types.VARCHAR);
        callableStatement.executeQuery();
		String customerLevel=callableStatement.getString(2);
		
		System.out.println(customerLevel);
		
		statement= connection.createStatement();
		String s=" select case when creditlimit>50000 then 'PLATINUM' when creditlimit>=10000 and creditlimit<=50000 then 'GOLD' when creditlimit<10000 then 'SILVER' end as customerlevel from customers where customernumber="+data+"";
		result= statement.executeQuery(s);
		
		result.next();
		String expCustomerLevel=result.getString("customerlevel");
		System.out.println(expCustomerLevel);
		
		if(customerLevel.equalsIgnoreCase(expCustomerLevel) ) {
			assertTrue(true);
		}
		else
		assertTrue(false); 
		
		
	}
	public boolean compareResultSets(ResultSet resultSet1,ResultSet resultSet2) throws SQLException {
		
		while(resultSet1.next()) {
			resultSet2.next();
			int count=resultSet1.getMetaData().getColumnCount();
			for (int i = 1; i <= count; i++) {
				// can use String class to compare but it wont compare NULL values so opting for StringUtils class
				if(!StringUtils.equals(resultSet1.getString(i),resultSet2.getString(i))) {
					return false;
				}
				
			}
		}
		return true;
		
	}
}
