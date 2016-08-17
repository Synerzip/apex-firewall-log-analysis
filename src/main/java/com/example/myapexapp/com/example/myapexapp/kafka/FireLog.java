package com.example.myapexapp.com.example.myapexapp.kafka;

/**
 * Created by Akshay Harale<akshay.harale@synerzip.com/> on 16/6/16.
 */
public class FireLog {

    String bu;
    String userIp;
    String destinationIp;
    Long time;
    String userId;

    public String getBu() {
        return bu;
    }

    public void setBu(String bu) {
        this.bu = bu;
    }

    public String getUserIp() {
        return userIp;
    }

    public void setUserIp(String userIp) {
        this.userIp = userIp;
    }

    public String getDestinationIp() {
        return destinationIp;
    }

    public void setDestinationIp(String destinationIp) {
        this.destinationIp = destinationIp;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

}
