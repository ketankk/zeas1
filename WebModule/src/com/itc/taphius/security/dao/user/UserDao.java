package com.itc.taphius.security.dao.user;


import org.springframework.security.core.userdetails.UserDetailsService;

import com.itc.taphius.security.dao.Dao;
import com.itc.taphius.security.entity.User;


public interface UserDao extends Dao<User, Long>, UserDetailsService
{

	User findByName(String name);

}