package com.ak.hive.ddl.extract;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, SQLException
    {
        System.out.println( "Connection Test" );
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con = DriverManager.getConnection("");
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("");
        while(rs.next()){
        	rs.getString(1);
        }
         
       System.out.println("Done");
    }
}
