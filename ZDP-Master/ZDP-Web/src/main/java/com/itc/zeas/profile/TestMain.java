package com.itc.zeas.profile;

import com.itc.zeas.utility.connection.ConnectionUtility;

import java.sql.*;

/**
 * @author Ketan on 5/1/2017.
 */
public class TestMain {
public static void main(String[] args) throws SQLException {

    Connection conn= ConnectionUtility.getConnection();
    PreparedStatement pstmt =
            conn.prepareStatement("select * from user where id in (?)");
    Array array = conn.createArrayOf("VARCHAR", new Object[]{"admin", "chaitra"});
    pstmt.setArray(1, array);
    ResultSet rs = pstmt.executeQuery();
}
}
