package com.itc.taphius.security.rest.resources;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.itc.taphius.dao.UserService;
import com.itc.taphius.security.rest.TokenUtils;
import com.itc.taphius.security.transfer.TokenTransfer;
import com.itc.taphius.security.transfer.UserTransfer;


//@Component
//@Path("/rest/user")
@RestController
@RequestMapping("/rest/user")
public class UserResource
{

	@Autowired
	private UserDetailsService userService;

	@Autowired
	@Qualifier("authenticationManager")
	private AuthenticationManager authManager;

	/**
	 * Retrieves the currently logged in user.
	 * 
	 * @return A transfer containing the username and the roles.
	 */
//	@GET
//	@Produces(MediaType.APPLICATION_JSON)
	@RequestMapping(method = RequestMethod.GET,headers="Accept=application/json")
	public UserTransfer getUser()
	{
		Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
		Object principal = authentication.getPrincipal();
		System.out.println("inside getUser method =="+principal);
		if (principal instanceof String && ((String) principal).equals("anonymousUser")) {
			return new UserTransfer("anonymousUser", new HashMap<String, Boolean>());
		}
		UserDetails userDetails = (UserDetails) principal;

		return new UserTransfer(userDetails.getUsername(), this.createRoleMap(userDetails));
	}


	/**
	 * Authenticates a user and creates an authentication token.
	 * 
	 * @param username
	 *            The name of the user.
	 * @param password
	 *            The password of the user.
	 * @return A transfer containing the authentication token.
	 */
	//@Path("authenticate")
	//@POST
	//@Produces(MediaType.APPLICATION_JSON)
	@RequestMapping(value ="/authenticate", method = RequestMethod.POST,headers="Accept=application/json")
	public TokenTransfer authenticate(@RequestParam("username") String username, @RequestParam("password") String password)
	{
		System.out.println("authenticate ==="+username+"000"+password);
		UsernamePasswordAuthenticationToken authenticationToken =
				new UsernamePasswordAuthenticationToken(username, password);
		Authentication authentication = this.authManager.authenticate(authenticationToken);
		SecurityContextHolder.getContext().setAuthentication(authentication);

		/*
		 * Reload user as password of authentication principal will be null after authorization and
		 * password is needed for token generation
		 */
		UserDetails userDetails = this.userService.loadUserByUsername(username);

		return new TokenTransfer(TokenUtils.createToken(userDetails));
	}


	private Map<String, Boolean> createRoleMap(UserDetails userDetails)
	{
		Map<String, Boolean> roles = new HashMap<String, Boolean>();
		for (GrantedAuthority authority : userDetails.getAuthorities()) {
			roles.put(authority.getAuthority(), Boolean.TRUE);
		}

		return roles;
	}
	
	@RequestMapping(value ="/forgetPassword/{userName}", method = RequestMethod.POST,headers="Accept=application/json")
	public @ResponseBody void forgetPassword(@PathVariable("userName") String UserName){
		UserService userService= new UserService();	
		userService.forgetPassword(UserName);
	}

}