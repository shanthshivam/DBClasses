package com.java.jdbc;

import java.io.File;
import java.sql.*;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.events.StartDocument;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class XMLData {
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

//		        returnXML(conn);
//				insertRecords(conn);
				returnRulesAsXML(conn);
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

	public static void returnXML(Connection conn) {
		
		try {
			 // Create a statement
            Statement stmt = conn.createStatement();
            // Execute a query
            String sql = "select a.token, a.attr, a.field_Type,a.group_token,c.token as \"query_token\", a.is_must as \"conditions_is_must\",d.token as \"query_group_token\",d.title,d.is_must,a.operator,a.props  from zeronsec.conditions a, zeronsec.alerts  b, zeronsec.queries c, zeronsec.query_groups d where b.token = c.alert_token and c.token = d.query_token and d.token = a.group_token;";
            ResultSet rs = stmt.executeQuery(sql);
            
            // Initialize a new document
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            
            // Create the root element
            Element rootElement = doc.createElement("RULES");
            doc.appendChild(rootElement);

            int columnCount = rs.getMetaData().getColumnCount();
            // Process the result set
            while (rs.next()) {
            	
            	
                Element rule = doc.createElement("RULE");
                rootElement.appendChild(rule);
                for(int i=1; i<= columnCount; i ++) {

                  Element id = doc.createElement(rs.getMetaData().getColumnName(i));
                  id.appendChild(doc.createTextNode(rs.getString(rs.getMetaData().getColumnName(i))));
                  rule.appendChild(id);
                  
                }
//
//                Element id = doc.createElement("ID");
//                id.appendChild(doc.createTextNode(rs.getString("id")));
//                employee.appendChild(id);
//
//                Element name = doc.createElement("Name");
//                name.appendChild(doc.createTextNode(rs.getString("name")));
//                employee.appendChild(name);

                // Add more fields as necessary...
            }
            
            // Write the content into XML file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
         // Enable indentation (pretty-print)
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File("d:/Rules.xml"));
            
            transformer.transform(source, result);
            
            // Close the connection
            conn.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	

	public static void returnRulesAsXML(Connection conn) {
		
		try {
			 // Create a statement
            Statement stmt = conn.createStatement();
            
            // Execute a query
            String sql = "select token from queries";
            ResultSet rs = stmt.executeQuery(sql);
            
            // Initialize a new document
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            
            // Create the root element
            Element rootElement = doc.createElement("RULES");
            doc.appendChild(rootElement);
            

            int columnCount = rs.getMetaData().getColumnCount();
            // Process the result set
            while (rs.next()) {
            	
            	
                Element rule = doc.createElement("RULE");
                rootElement.appendChild(rule);
//                for(int i=1; i<= columnCount; i ++) {
//
//                  Element id = doc.createElement(rs.getMetaData().getColumnName(i));
//                  id.appendChild(doc.createTextNode(rs.getString(rs.getMetaData().getColumnName(i))));
//                  rule.appendChild(id);
//                  
//                }
                
                Element token = doc.createElement("TOKEN");
                token.appendChild(doc.createTextNode(rs.getString("token")));
                Element alertToken = doc.createElement("ALERT_TOKEN");
                alertToken.appendChild(doc.createTextNode(rs.getString("token")));
                rule.appendChild(token);
                rule.appendChild(alertToken);
                
                returnXMLQueryGroup(doc, rule, "GROUP", "SELECT * FROM QUERY_GROUPS WHERE QUERY_TOKEN='" + rs.getString("token")+ "'", conn);
            }
            
            // Write the content into XML file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
         // Enable indentation (pretty-print)
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File("d:/Rules.xml"));
            
            transformer.transform(source, result);
            
            // Close the connection
            conn.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static void returnXMLQueryGroup(Document doc, Element rule, String tag, String sql, Connection conn) {
		
		try {
			 // Create a statement
            Statement stmt = conn.createStatement();
            
            // Execute a query
            //sql = "select a.token, a.attr, a.field_Type,a.group_token,c.token as \"query_token\", a.is_must as \"conditions_is_must\",d.token as \"query_group_token\",d.title,d.is_must,a.operator,a.props  from zeronsec.conditions a, zeronsec.alerts  b, zeronsec.queries c, zeronsec.query_groups d where b.token = c.alert_token and c.token = d.query_token and d.token = a.group_token;";
            ResultSet rs = stmt.executeQuery(sql);
            int columnCount = rs.getMetaData().getColumnCount();
            // Process the result set
            while (rs.next()) {
            	
            	
                Element group = doc.createElement(tag);
                rule.appendChild(group);
                Element token = doc.createElement("TOKEN");
                token.appendChild(doc.createTextNode(rs.getString("token")));
                Element isMust = doc.createElement("IS_MUST");
                isMust.appendChild(doc.createTextNode(rs.getString("is_must")));
                Element title = doc.createElement("TITLE");
                title.appendChild(doc.createTextNode(rs.getString("title")));
                Element position = doc.createElement("POSITION");
                position.appendChild(doc.createTextNode(rs.getString("position")));
                group.appendChild(token);
                group.appendChild(isMust);
                group.appendChild(title);
                group.appendChild(position);
                System.out.println("GROUPS " + rs.getString("token")); 	
                returnXMLQueryConditions(doc, group, "CONDITION", "SELECT * FROM CONDITIONS WHERE GROUP_TOKEN='"+rs.getString("token")+"'", conn);
                
            }
            
            
            // Close the connection
            stmt.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void returnXMLQueryConditions(Document doc, Element group, String tag, String sql, Connection conn) {
		
		try {
			 // Create a statement
            Statement stmt = conn.createStatement();
            
            // Execute a query
            ResultSet rs = stmt.executeQuery(sql);
            int columnCount = rs.getMetaData().getColumnCount();
            // Process the result set
            while (rs.next()) {
            	
            	
                Element condition = doc.createElement(tag);
                group.appendChild(condition);
                for(int i=1; i<= columnCount; i ++) {

                  Element id = doc.createElement(rs.getMetaData().getColumnName(i));
                  System.out.println(rs.getMetaData().getColumnName(i) + " is added  " + rs.getString(rs.getMetaData().getColumnName(i)));
                  id.appendChild(doc.createTextNode(rs.getString(rs.getMetaData().getColumnName(i))));
                  condition.appendChild(id);
                  
                }
            }
            
            
            // Close the connection
            stmt.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
// end FirstExample