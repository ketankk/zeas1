package com.itc.zeas.ingestion.automatic.bulk;



import javax.servlet.http.HttpServletRequest;

import com.itc.zeas.profile.model.Entity;

import java.util.List;

public interface EntityManagerInterface {
    List<Entity> getEntity(String type, HttpServletRequest httpRequest) throws Exception;

    void addEntity(Entity entity, HttpServletRequest request) throws Exception;


}
