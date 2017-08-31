package com.itc.taphius.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.crypto.password.StandardPasswordEncoder;

import com.itc.taphius.model.Roles;
import com.itc.taphius.model.UserDetails;
import com.itc.taphius.utility.DBUtility;

public class UserService {

	/**
	 * Using spring StandardPasswordEncoder to encode the password
	 */
	private PasswordEncoder passwordEncoder = new StandardPasswordEncoder("ThisIsASecretSoChangeMe");
	private Connection connection;
	Properties prop = new Properties();

	public UserService() {
		connection = DBUtility.getConnection();
	}

	/**
	 * this method is used to list all the existing users
	 * @return userList
	 */
	public List<UserDetails> getUsers() {
		List<UserDetails> userList = new ArrayList<UserDetails>();

		try {
			Statement statement = connection.createStatement();
			String sQuery = DBUtility.getSQlProperty("LIST_USERS");
			ResultSet rs = statement.executeQuery(sQuery);
			while (rs.next()) {
				UserDetails userDetails = new UserDetails();
				Roles roles = new Roles();
				userDetails.setUserId(rs.getInt("id"));
				userDetails.setUserName(rs.getString("name"));
				userDetails.setPassword(rs.getString("password"));
				userDetails.setEmail(rs.getString("email"));
				userDetails.setDisplayName(rs.getString("display_name"));
				roles.setRolesName(rs.getString("roles"));
				userDetails.setRoles(roles);
				userList.add(userDetails);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return userList;
	}

	/**
	 * This method is get user details by user id
	 * @param id
	 * @return UserDetails
	 */
	public UserDetails getUserById(Integer id) {
		UserDetails userDetails = null;

		try {
			String sQuery = DBUtility.getSQlProperty("SELECT_USER_BY_ID");
			PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, id);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				userDetails = new UserDetails();
				Roles roles = new Roles();

				userDetails.setUserId(rs.getInt("id"));
				userDetails.setUserName(rs.getString("name"));
				userDetails.setPassword(rs.getString("password"));
				userDetails.setEmail(rs.getString("email"));
				userDetails.setDisplayName(rs.getString("display_name"));
				roles.setRolesName(rs.getString("roles"));
				userDetails.setRoles(roles);
				System.out.println("chkQ" + userDetails);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return userDetails;
	}

	/**
	 * This method is to add new user
	 * @param userDetails
	 */
	public void addUsers(UserDetails userDetails) {
		try {
			String sQuery = DBUtility.getSQlProperty("INSERT_USER");
			PreparedStatement preparedStatement1 = connection.prepareStatement(sQuery);
			preparedStatement1.setString(1, userDetails.getUserName());
			preparedStatement1.setString(2,passwordEncoder.encode(userDetails.getPassword()));
			preparedStatement1.setString(3, userDetails.getEmail());
			preparedStatement1.setString(4, userDetails.getDisplayName());
			preparedStatement1.executeUpdate();

			String srQuery = DBUtility.getSQlProperty("INSERT_ROLES");
			System.out.println("chkQ" + preparedStatement1);
			PreparedStatement preparedStatemen2 = connection.prepareStatement(srQuery);
			preparedStatemen2.setInt(1, getUserID(userDetails.getUserName()));
			preparedStatemen2.setString(2, userDetails.getRoles().getRolesName());
			preparedStatemen2.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * this method is to update user
	 * @param userDetails
	 * @param userId
	 */
	public void updateUsers(UserDetails userDetails, Integer userId) {
		try {
			String sQuery = DBUtility.getSQlProperty("UPDATE_USER");
			PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, userDetails.getUserName());
			preparedStatement.setString(2,passwordEncoder.encode(userDetails.getPassword()));
			preparedStatement.setString(3, userDetails.getEmail());
			preparedStatement.setString(4, userDetails.getDisplayName());
			preparedStatement.setString(5, userDetails.getRoles().getRolesName());
			preparedStatement.setInt(6, userId);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * this method is used to delete users
	 * @param userId
	 */
	public void deleteUsers(Integer userId) {
		try {

			String sQuery = DBUtility.getSQlProperty("DELETE_USER_ROLES");
			PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setInt(1, userId);
			preparedStatement.executeUpdate();

			String query = DBUtility.getSQlProperty("DELETE_USER");
			PreparedStatement preStatement = connection.prepareStatement(query);
			preStatement.setInt(1, userId);
			preStatement.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * this method is to return userid using unique username
	 * @param userName
	 * @return userId
	 */
	public int getUserID(String userName) {
		int userId = 0;
		try {

			String sQuery = DBUtility.getSQlProperty("GET_USER_ID");
			PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, userName);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				userId = rs.getInt("id");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return userId;
	}

	/**
	 * this method is to update password
	 * @param userDetails
	 * @param userName
	 */
    public void updatePassword(UserDetails userDetails, String userName) {
    	try {
        	String sQuery = DBUtility.getSQlProperty("UPDATE_USER_PASSWORD");
            PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
            preparedStatement.setString(1,passwordEncoder.encode(userDetails.getNewpassword()));
            preparedStatement.setString(2, userName);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
        	e.printStackTrace();
        }
    }

	/**
	 * this method is for forget password
	 * @param userDetails
	 */
	public void forgetPassword(String userName) {
		try {
			String sQuery = DBUtility.getSQlProperty("UPDATE_USER_PASSWORD");
			PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, passwordEncoder.encode("login@123"));
			preparedStatement.setString(2, userName);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is to check user name availability
	 * @param userName
	 * @return exists
	 */
	public boolean checkAvailability(String userName) {
		boolean exists = false;
		
		try {
			String sQuery = DBUtility.getSQlProperty("CHECK_USER_AVAILABILITY");
			PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, userName);
			ResultSet rs  = preparedStatement.executeQuery();
			
			if (rs.next()) {
				exists = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}	
		return exists;
	}
	
	/**
	 * this method is to get user password from database
	 * @param userName
	 * @return password
	 */
	public String getUserPassword(String userName) {
		String password = null;
		
		try {
			String sQuery = DBUtility.getSQlProperty("GET_USER_PASSWORD");
			PreparedStatement preparedStatement = connection.prepareStatement(sQuery);
			preparedStatement.setString(1, userName);
			ResultSet rs  = preparedStatement.executeQuery();
			
			while (rs.next()) {
				password = rs.getString("password");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}	
		return password;
	}

}
