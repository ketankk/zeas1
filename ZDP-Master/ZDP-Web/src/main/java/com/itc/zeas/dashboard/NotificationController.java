package com.itc.zeas.dashboard;

import java.sql.SQLException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.itc.zeas.exceptions.ZeasErrorCode;
import com.itc.zeas.exceptions.ZeasException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.itc.zeas.dashboard.daoimpl.NotificationService;
import com.itc.zeas.utility.utils.CommonUtils;
import com.itc.zeas.dashboard.model.NotificationInfo;

@RestController
@RequestMapping("/rest/service")
public class NotificationController {


    @RequestMapping(value = "/getNotifications", method = RequestMethod.GET, headers = "Accept=application/json")
    public
    @ResponseBody
    ResponseEntity<?> getNotifications(HttpServletRequest httpServletRequest) throws SQLException {
        try {
            CommonUtils commonUtils = new CommonUtils();
            String accessToken = commonUtils
                    .extractAuthTokenFromRequest(httpServletRequest);
            String userName = commonUtils.getUserNameFromToken(accessToken);

            List<NotificationInfo> listOfNotifications;
            NotificationService notificationService = new NotificationService();
            listOfNotifications = notificationService.getLatestNotification(userName);
            return ResponseEntity.ok(listOfNotifications);
        } catch (Exception e) {
            return ResponseEntity.status(ZeasErrorCode.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
        }

    }

    @RequestMapping(value = "/getNotificationCount", method = RequestMethod.GET, headers = "Accept=application/json")
    public
    @ResponseBody
    Integer getNotificationCount(HttpServletRequest httpServletRequest) throws SQLException {

        CommonUtils commonUtils = new CommonUtils();
        String accessToken = commonUtils
                .extractAuthTokenFromRequest(httpServletRequest);
        String userName = commonUtils.getUserNameFromToken(accessToken);
        NotificationService notificationService = new NotificationService();
        int numberOfNotifications = notificationService.getNoOfNotifications(userName);
        return numberOfNotifications;
    }


}
