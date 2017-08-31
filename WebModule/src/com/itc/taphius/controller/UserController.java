package com.itc.taphius.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.crypto.password.StandardPasswordEncoder;

import com.itc.taphius.dao.UserService;
import com.itc.taphius.model.UserDetails;


/**
 * @author 16765
 * 
 */
@RestController
@RequestMapping("/rest/service")
public class UserController {
	private PasswordEncoder passwordEncoder = new StandardPasswordEncoder("ThisIsASecretSoChangeMe");
	boolean resetpassword=false;
	@RequestMapping(value ="/listUsers", method = RequestMethod.GET,headers="Accept=application/json")
	public List<UserDetails> getUsers() {
		UserService userService= new UserService();		
		List<UserDetails> users=userService.getUsers();
		return users;

	}
	
	@RequestMapping(value ="/addUser", method = RequestMethod.POST,headers="Accept=application/json")
	public @ResponseBody UserDetails addUsers(@RequestBody  UserDetails userDetails) {
		UserService userService= new UserService();		
		userService.addUsers(userDetails);
		return userDetails;
	}
	
	@RequestMapping(value ="/updateUser/{userId}", method = RequestMethod.POST,headers="Accept=application/json")
	public @ResponseBody UserDetails updateUser(@RequestBody  UserDetails userDetails,@PathVariable("userId") Integer userId){
		UserService userService= new UserService();		
		userService.updateUsers(userDetails,userId);
		return userDetails;
	}
	
	@RequestMapping(value ="/deleteUser/{userId}", method = RequestMethod.DELETE,headers="Accept=application/json")
	public @ResponseBody void deleteUser(@PathVariable("userId") Integer userId){
		UserService userService= new UserService();		
		userService.deleteUsers(userId);
	}
	
	@RequestMapping(value="/getUser/{userId}", method = RequestMethod.GET,headers="Accept=application/json")
	public @ResponseBody UserDetails getUserById(@PathVariable("userId") Integer userId) {
		UserService userService = new UserService();
		UserDetails dtl = userService.getUserById(userId);
		return dtl;

	}
	
	@RequestMapping(value ="/updatePassword/{userName}", method = RequestMethod.POST,headers="Accept=application/json")
    public @ResponseBody UserDetails updatePassword(@RequestBody  UserDetails userDetails,@PathVariable("userName") String UserName){
		UserService userService= new UserService();    
		String userDbPassword = userService.getUserPassword(UserName);
		CharSequence rawPassword = userDetails.getPassword();
		
        if ((passwordEncoder.matches(rawPassword,userDbPassword))) {
        	userService.updatePassword(userDetails,UserName);
            resetpassword=true;
        } 
        return userDetails;     
	}
	
	@RequestMapping(value ="/CheckUserNameAvailability/{userName}", method = RequestMethod.POST,headers="Accept=application/json")
	public @ResponseBody boolean CheckUserNameAvailability(@PathVariable("userName") String UserName){
		UserService userService= new UserService();	
		boolean exists = userService.checkAvailability(UserName);
		return exists;
	}
}
