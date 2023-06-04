package com.java.jdbc;

import java.sql.*;
import java.util.Random;

import javax.xml.stream.events.StartDocument;

public class DBIPInsert {
	// JDBC driver name and database URL
//   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
//   static final String DB_URL = "jdbc:mysql://localhost/EMP";
//
//   //  Database credentials
//   static final String USER = "username";
//   static final String PASS = "password";

	static final String JDBC_DRIVER = ConfigProperties.getProperty("JDBC_DRIVER");
	static final String DB_URL = ConfigProperties.getProperty("DB_URL");

	// Database credentials
	static final String USER = ConfigProperties.getProperty("USER");
	static final String PASS = ConfigProperties.getProperty("PASS");

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		long startTime = System.currentTimeMillis();
		try {
			// Register JDBC driver
			Class.forName(JDBC_DRIVER);

			// Open a connection
			System.out.println("Connecting to database..." + JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			conn.setAutoCommit(false);
			
			boolean action = false;

			if (action) {

				// Execute a query
				System.out.println("Creating statement...");
				stmt = conn.createStatement();
				String sql;
				sql = "SELECT id, name, role FROM log_data";
				ResultSet rs = stmt.executeQuery(sql);

				// Extract data from result set
				while (rs.next()) {
					// Retrieve by column name
					int id = rs.getInt("id");
					String name = rs.getString("name");
					String role = rs.getString("role");

					// Display values
					System.out.print("ID: " + id);
					System.out.print(", First: " + name);
					System.out.println(", Last: " + role);
				}
				// Clean-up environment
				rs.close();
				stmt.close();
				conn.close();

			} else {

		        
				insertRecords(conn);
			}
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			} // nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
		} // end try
		System.out.println("Goodbye! " + (System.currentTimeMillis() - startTime) + " msecs");
	}// end main

	public static void insertRecords(Connection conn) {

		try {
			long startTime = System.currentTimeMillis();
			// Execute a query
			System.out.println("Creating statement...");
			Statement stmt = conn.createStatement();
			String sql = null;
			Random rand = new Random();

			PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO fortigate(id, srcip, destip,srcport,destport,timestamp) VALUES (?,?,?,?,?,?)");

			System.out.println("Sql " + sql);
			int i = 373759;
			int count = 1;
			ResultSet rs = stmt.executeQuery("select count(*) from fortigate");
			while(rs.next()) {
				count = rs.getInt(1);
			}
			i = count +1;
			while (i < ((count) + 50000)) {

				if (rand.nextBoolean()) {

					//System.out.println("Generating Normal event" );
					ps.setInt(1, i);
					int randomNumber = (rand.nextInt(100) + 1);
					ps.setString(2, "10.101." + randomNumber + "." + (rand.nextInt(100) + 1));
					ps.setString(3, "10.201." + randomNumber + "." + (rand.nextInt(100) + 1));
					ps.setString(4, "" + (rand.nextInt(100) + 1));
					ps.setString(5, "" + (rand.nextInt(100) + 1));
					ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
					ps.addBatch();

				} else {

					//System.out.println("Generating sequence of alert event ");
					int randomNumber = (rand.nextInt(100) + 1);
					// randomly generate an alert
					for (int j = 0; j < 6; j++) {

						ps.setInt(1, i);

						//System.out.println("Generating sequence of alert event " + randomNumber );
						ps.setString(2, "10.101." + randomNumber + "." + randomNumber);
						ps.setString(3, "10.201." + randomNumber + "." + randomNumber);
						ps.setString(4, ""+j);
						ps.setString(5, ""+j);
						ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
						ps.addBatch();
					}

				}
//				System.out.println("No of records inserted " + i);
				i++;
//				if((i % 1000) ==0) {
//					System.out.println("No of records inserted " + i);
//					
//					//ps.executeBatch();
//					// perform insert operations
//					//conn.commit();
//					//ps.close();
////					 ps = conn.prepareStatement(
////								"INSERT INTO fortigate(id, srcip, destip,srcport,destport,timestamp) VALUES (?,?,?,?,?,?)");
//				}
			}

			System.out.println("Commiting the batch");
			ps.executeBatch();
			conn.commit();
			// stmt.executeBatch();
			System.out.println(i + "  records inserted " + (System.currentTimeMillis() - startTime));
			// Clean-up environment
			ps.close();
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
		} // end try
	}

}
// end FirstExample