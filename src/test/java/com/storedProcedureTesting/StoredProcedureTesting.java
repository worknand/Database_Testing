package com.storedProcedureTesting;

import static org.testng.Assert.assertFalse;
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





public class StoredProcedureTesting {
	
	Connection connection=null;
	Statement statement=null;
	ResultSet result;
	CallableStatement callableStatement;
	ResultSet resultSet1;
	ResultSet resultSet2;
	
	
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
	void testStoredProceduresTc01() throws SQLException {
		
		statement=connection.createStatement();
		result= statement.executeQuery("SHOW PROCEDURE STATUS WHERE Name='SelectAllCustomers'");
		result.next() ;
		String name=result.getString("Name");
		System.out.println(name);
		
		Assert.assertEquals(name,"SelectAllCustomers");
			
		/*	int customerNumber=result.getInt("customerNumber");
			String customerName=result.getString("customerName");
			String contactLastName=result.getString("contactLastName");
			String contactFirstName=result.getString("contactFirstName");
			String phone=result.getString("phone");
			String addressLine1=result.getString("addressLine1");
			String addressLine2=result.getString("addressLine2");
			String city=result.getString("city");
			String state=result.getString("state");
			String postalCode=result.getString("postalCode");
			String country=result.getString("country");
			int salesRepEmployeeNumber=result.getInt("salesRepEmployeeNumber");
			float creditLimit=result.getFloat("creditLimit");  */
		
	}
	
	
	@Test(priority = 2)
	void SelectAllCustomersTc02() throws SQLException {
		
		callableStatement=connection.prepareCall("{CALL SelectAllCustomers()}");
		resultSet1=callableStatement.executeQuery();                     // resultset1
		
		statement= connection.createStatement();
		String s="Select * from customers";
		resultSet2= statement.executeQuery(s);    //resultset2
		
	boolean compareResult=compareResultSets(resultSet1, resultSet2);
	Assert.assertEquals(compareResult, true, "Exact match");
	System.out.println("Exact match");
		
	}
	
	
	@Test(priority = 3)
	void SelectAllCustomersByCityTc03() throws SQLException {
		
		callableStatement=connection.prepareCall("{CALL SelectAllCustomersByCity(?)}");
		callableStatement.setString(1, "singapore");
		resultSet1=callableStatement.executeQuery();                     // resultset1
		
		statement= connection.createStatement();
		String s="Select * from customers where city='singapore'";
		resultSet2= statement.executeQuery(s);    //resultset2
		
	boolean compareResult=compareResultSets(resultSet1, resultSet2);
	Assert.assertEquals(compareResult, true, "Exact match");
	System.out.println("Exact match");
		
	}
	
	@Test(priority = 4)
	void SelectAllByCustomersbyCityAndPinTc04() throws SQLException {
		
		callableStatement=connection.prepareCall("{CALL SelectAllByCustomersbyCityAndPin(?,?)}");
		callableStatement.setString(1, "singapore");
		callableStatement.setString(2, "83030");
		resultSet1=callableStatement.executeQuery();                     // resultset1
		
		statement= connection.createStatement();
		String s="Select * from customers where city='singapore' and postalCode='83030'";
		resultSet2= statement.executeQuery(s);    //resultset2
		
	boolean compareResult=compareResultSets(resultSet1, resultSet2);
	Assert.assertEquals(compareResult, true, "Exact match");
	System.out.println("Exact match");
		
	}
	
	@Test(priority=5)
	void get_order_by_custTc05() throws SQLException {
		
		callableStatement=connection.prepareCall("{call get_order_by_cust(?,?,?,?,?)}");
		callableStatement.setInt(1, 141);
		//setting output parameters
		callableStatement.registerOutParameter(2, Types.INTEGER);
		callableStatement.registerOutParameter(3, Types.INTEGER);
		callableStatement.registerOutParameter(4, Types.INTEGER);
		callableStatement.registerOutParameter(5, Types.INTEGER);
		
		
		callableStatement.executeQuery();
		
		int shipped=callableStatement.getInt(2);
		int canceled=callableStatement.getInt(3);
		int resolved=callableStatement.getInt(4);
		int disputed=callableStatement.getInt(5);
		
		System.out.println(shipped+"  "+canceled+"  "+resolved+"  "+disputed);
		
		statement= connection.createStatement();
		String s="select ( select count(*) as shipped from orders where customerNumber=141 and status='Shipped') as shipped,( select count(*) as canceled from orders where customerNumber=141 and status='canceled') as canceled,( select count(*) as resolved from orders where customerNumber=141 and status='resolved') as resolved,( select count(*) as disputed from orders where customerNumber=141 and status='disputed') as disputed";
		result= statement.executeQuery(s);
		
		result.next();
		
		int expShipped=result.getInt("shipped");
		int expCanceled=result.getInt("canceled");
		int expResolved=result.getInt("resolved");
		int expDisputed=result.getInt("disputed");
		
		System.out.println(expShipped+"  "+expCanceled+"  "+expResolved+"  "+expDisputed);
		
		if(shipped==expShipped && canceled==expCanceled && resolved==expResolved && disputed==expDisputed) {
			assertTrue(true);
		}
		else
		assertTrue(false);
		
	}
	@Test(priority = 6)
	public void callmethodTC06() throws SQLException {
		GetCustomerShippingTc06(112);
		GetCustomerShippingTc06(202);
		GetCustomerShippingTc06(121);
	}
	
	public void GetCustomerShippingTc06(int data) throws SQLException {
		
		callableStatement=connection.prepareCall("{call GetCustomerShipping(?,?)}");
		callableStatement.setInt(1, data);  //112, 202,186
		//setting output parameters
		callableStatement.registerOutParameter(2, Types.VARCHAR);
		
		
		
		callableStatement.executeQuery();
		
		String shippingTime=callableStatement.getString(2);
		
		System.out.println(shippingTime);
		
		statement= connection.createStatement();
		String s="select country,case when country='USA' then '2-day shipping' when country='canada' then '3-day shipping' else '5-day shipping' end as ShippingTime from customers where customerNumber="+data+"";
		result= statement.executeQuery(s);
		
		result.next();
		
		String expShippingTime=result.getString("ShippingTime");
		
		
		System.out.println(expShippingTime);
		
		if(shippingTime.equalsIgnoreCase(expShippingTime) ) {
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
