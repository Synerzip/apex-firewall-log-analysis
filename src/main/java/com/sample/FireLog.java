package com.sample;

/**
 * Created by Akshay Harale<akshay.harale@synerzip.com/> on 16/6/16.
 */
public class FireLog {

//    bu: String,
//    userIp: String,
//    destinationIp: String,
//    time: Long,
//    userId: String,
//    sentBytes: Long,
//    recvdBytes: Long,
//    uuid: String = UUID.randomUUID().toString

    private String bu;
    private String userIp;
    private String destinationIp;
    private Long time;
    private String userId;
    private Long sentBytes;
    private Long rcvdBytes;
    private String uuid;
    private Tuple bytes=new Tuple(0l,0l,0l);

    public Tuple getBytes() {
        return bytes;
    }

    public void setBytes(Tuple bytes) {
        this.bytes.rcvdBytes = getRcvdBytes();
        this.bytes.sentBytes = getSentBytes();
        this.time=getTime();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }


    public Long getSentBytes() {
        return sentBytes;
    }

    public void setSentBytes(Long sentBytes) {
        this.sentBytes = sentBytes;
        this.bytes.sentBytes = sentBytes;
    }

    public Long getRcvdBytes() {
        return rcvdBytes;
    }

    public void setRcvdBytes(Long rcvdBytes) {
        this.rcvdBytes = rcvdBytes;
        this.bytes.rcvdBytes = rcvdBytes;
    }

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
        this.bytes.time=time;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}