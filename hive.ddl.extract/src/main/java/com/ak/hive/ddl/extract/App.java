package com.ak.hive.ddl.extract;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.security.UserGroupInformation;


public class App 
{
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Connection Test" );
        
        org.apache.hadoop.conf.Configuration conf = new     org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("ambari-qa@EXAMPLE.COM", "/etc/security/keytabs/smokeuser.headless.keytab");
        
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con = DriverManager.getConnection("");
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("");
        while(rs.next()){
        	System.out.println(rs.getString(1));
        }
         
       System.out.println("Done");
    }
}
