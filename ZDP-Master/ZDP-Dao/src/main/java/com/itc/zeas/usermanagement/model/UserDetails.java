package com.itc.zeas.usermanagement.model;

import lombok.Data;

@Data
public class UserDetails {
	private int userId;
	private String userName;
	private String password;
	private String firstName;
	private String lastName;
	private String displayName;
	private Roles roles;
	private String newpassword;
	private String email;
	private Address address;
	private String retypepassword;

}
