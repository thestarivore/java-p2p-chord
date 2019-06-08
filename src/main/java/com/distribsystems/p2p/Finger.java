package com.distribsystems.p2p;

import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigInteger;

public class Finger {
    private String      ipAddr;
    private int         port;
    private BigInteger  id;

    public Finger(String ipAddress, int port) {
        this.ipAddr = ipAddress;
        this.port   = port;

        //Create the ID by hashing "IP_ADDRESS:PORT"
        String hex = DigestUtils.sha1Hex(ipAddress + ":" + String.valueOf(port));
        BigInteger baseTwo = BigInteger.valueOf(2L);
        this.id = new BigInteger(hex, 16);
        this.id = this.id.mod(baseTwo.pow(Chord.FINGER_TABLE_SIZE));
    }

    public String getIpAddr() {
        return ipAddr;
    }

    public int getPort() {
        return port;
    }

    public BigInteger getId() {
        return id;
    }
}
