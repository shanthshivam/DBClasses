package com.java.jdbc;

import java.sql.*;
import java.util.Random;

import javax.xml.stream.events.StartDocument;

public class DBDemo {
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

			PreparedStatement ps = conn.prepareStatement("INSERT INTO log_data(id, name, role) VALUES (?,?,?)");

			//int[] results = ps.executeBatch();

			System.out.println("Sql " + sql);
			// ResultSet rs = stmt.executeQuery(sql);
			int i = 1;
			while (i < 100) {
//				stmt.addBatch("INSERT INTO log_data(id, name, role) VALUES (" + i + ", 'SomeNumber"
//						+ (rand.nextInt(100) + 1) + "', 'SomeRole" + (rand.nextInt(100) + 1) + "' )");
				
				ps.setInt(1, i);
				int randomNumber = (rand.nextInt(100) + 1);
				ps.setString(2, "SomeName" + randomNumber);
				ps.setString(3, "SomeRole" + randomNumber);
				ps.addBatch();

				i++;
			}

			ps.executeBatch();
			//stmt.executeBatch();
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