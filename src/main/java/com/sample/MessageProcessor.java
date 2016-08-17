package com.sample;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.joda.time.DateTime;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Akshay Harale<akshay.harale@synerzip.com/> on 16/6/16.
 */
public class MessageProcessor extends BaseOperator {
    public final transient DefaultOutputPort<FireLog> output = new DefaultOutputPort<FireLog>();

    transient String buTime = "\\s\\((.*?)\\)";
    transient String sourceDestination = "(?:[0-9]{1,3}\\.){3}[0-9]{1,3}";
    transient String uid = "\\\"\\S+@\\S+|\\{(?:\\w+, *)+\\w+\\}@[\\w.-]+";
    transient String keyValue = "([a-z,A-Z_]*)\\=\\\"(.*?)\\\"";
    transient Pattern butTimePattern = Pattern.compile(buTime);
    transient Pattern sourceDestinationPattern = Pattern.compile(sourceDestination);
    transient Pattern uidPattern = Pattern.compile(uid);
    transient Pattern keyValuePattern = Pattern.compile(keyValue);
    transient Matcher matcher;

    public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>() {


        @Override
        public void process(byte[] tuple) {
            try {
                if (tuple != null) {
                    Map<String, Object> logs = new HashMap<String, Object>();
                    FireLog fire = new FireLog();
                    String message = null;
                    try {
                        message = new String(tuple, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    if (message.contains("sent_bytes") && message.contains("rcvd_bytes")) {

                        matcher = keyValuePattern.matcher(message);
                        Map<String, String> keyValueMap = new HashMap<String, String>();
                        while (matcher.find()) {
                            String group = matcher.group();
                            String[] split = group.split("=");
                            String s = split[1];
                            keyValueMap.put(split[0], s.substring(1, s.length() - 1));
                        }

                        logs.put("sent_bytes", new Long(keyValueMap.get("sent_bytes")));
                        logs.put("rcvd_bytes", new Long(keyValueMap.get("rcvd_bytes")));

                        fire.setSentBytes(new Long(keyValueMap.get("sent_bytes")));
                        fire.setRcvdBytes(new Long(keyValueMap.get("rcvd_bytes")));

                        matcher = butTimePattern.matcher(message);
                        ArrayList<String> buTimeList = new ArrayList<>();
                        while (matcher.find()) {
                            String trim = matcher.group().trim();
                            if (trim.split("-").length > 1) {
                                buTimeList.add(trim);
                            }
                        }
                        if (buTimeList.size() == 0) {
                            logs.put("time", new DateTime().getMillis());
                            logs.put("bu", "nobu");
                            fire.setTime(new DateTime().getMillis());
                            fire.setBu("nobu");

                        } else {
                            String s = buTimeList.get(0);
                            Long time = new DateTime(s.substring(1, s.length() - 1)).getMillis();
                            logs.put("time", time);
                            fire.setTime(time);
                            if (buTimeList.size() == 1) {
                                logs.put("bu", "nobu");
                                fire.setBu("nobu");
                            } else {
                                String fullBuName = buTimeList.get(1);
                                // System.out.println("***********************************" + fullBuName + "********************************");
                                String bu = fullBuName.substring(1, fullBuName.indexOf("-"));
                                logs.put("bu", bu);
                                fire.setBu(bu);
                            }
                        }

                        ArrayList<String> sdList = new ArrayList<String>();
                        matcher = sourceDestinationPattern.matcher(message);
                        while (matcher.find()) {
                            sdList.add(matcher.group().trim());
                        }
                        if (sdList.size() == 0) {
                            logs.put("userIp", "");
                            logs.put("destinationIp", "");
                            fire.setUserIp("");
                            fire.setDestinationIp("");
                        } else {
                            if (sdList.size() == 1) {
                                String userIp = sdList.get(0);
                                logs.put("userIp", userIp);
                                logs.put("destinationIp", "");
                                fire.setUserIp(userIp);
                                fire.setDestinationIp("");
                            } else {
                                String userIp = sdList.get(0);
                                String destIp = sdList.get(1);
                                logs.put("userIp", userIp);
                                logs.put("destinationIp", destIp);
                                fire.setUserIp(userIp);
                                fire.setDestinationIp(destIp);
                            }
                        }

                        ArrayList<String> uidList = new ArrayList<String>();
                        matcher = uidPattern.matcher(message);
                        while (matcher.find()) {
                            uidList.add(matcher.group().trim());
                        }
                        if (uidList.size() == 0) {
                            logs.put("userId", "");
                            fire.setUserId("");
                        } else {
                            String uidString = uidList.get(0);
                            String uid = uidString.substring(1, uidString.indexOf('@'));
                            logs.put("userId", uid);
                            fire.setUserId(uid);
                        }
                        String uuid = UUID.randomUUID().toString();
                        logs.put("uuid", uuid);
                        fire.setUuid(uuid);
                        output.emit(fire);
                    }
                }
            } catch (Exception e) {
                String s = new String(tuple);
                System.out.println(s);
                e.printStackTrace();
            }

        }
    };
}

