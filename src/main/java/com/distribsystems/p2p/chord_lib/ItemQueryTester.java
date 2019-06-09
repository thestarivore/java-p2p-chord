package com.distribsystems.p2p.chord_lib;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class ItemQueryTester extends Thread {
    private final static boolean QUERY_TESTER_ENABLE = false;
    private final static int ITEM_TESTER_PORT_NUMBER = 8010;
    private Map<BigInteger, String> tempItemTable = new HashMap<>();
    private int portNumber;
    private Node node;

    /**
     * @brief   Fill the ItemTable with Fake Items just for Test purposes
     * @param   node
     */
    public ItemQueryTester(Node node) {
        this.portNumber = node.getPort();
        this.node       = node;

        //Only the node with port number chosen does this thing
        if(this.portNumber == ITEM_TESTER_PORT_NUMBER && QUERY_TESTER_ENABLE) {
            tempItemTable.put(new BigInteger("54"), "Item no. 54");
            tempItemTable.put(new BigInteger("24"), "Item no. 24");
            tempItemTable.put(new BigInteger("122"), "Item no. 122");
            tempItemTable.put(new BigInteger("121"), "Item no. 121");
            tempItemTable.put(new BigInteger("128"), "Item no. 128");
            tempItemTable.put(new BigInteger("142"), "Item no. 142");
            tempItemTable.put(new BigInteger("42"), "Item no. 42");
            tempItemTable.put(new BigInteger("72"), "Item no. 72");
            tempItemTable.put(new BigInteger("11"), "Item no. 11");
            tempItemTable.put(new BigInteger("23"), "Item no. 23");
            tempItemTable.put(new BigInteger("76"), "Item no. 76");
            tempItemTable.put(new BigInteger("88"), "Item no. 88");
            tempItemTable.put(new BigInteger("3"), "Item no. 3");
            tempItemTable.put(new BigInteger("1"), "Item no. 1");
            tempItemTable.put(new BigInteger("123"), "Item no. 123");
            tempItemTable.put(new BigInteger("212"), "Item no. 212");
            tempItemTable.put(new BigInteger("61"), "Item no. 61");
            tempItemTable.put(new BigInteger("4"), "Item no. 4");
            tempItemTable.put(new BigInteger("10"), "Item no. 10");
            tempItemTable.put(new BigInteger("43"), "Item no. 43");
            tempItemTable.put(new BigInteger("65"), "Item no. 65");
        }
    }

    @Override
    public void run() {
        //Only the node with port number chosen does this thing
        if(this.portNumber == ITEM_TESTER_PORT_NUMBER && QUERY_TESTER_ENABLE) {
            //Wait 30seconds before starting
            try {
                Thread.sleep(25000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Iterate all keys in the temp item table, and for each of them try to execute a PLACE_ITEM
            // on the right node in the ChordRing
            for (BigInteger key : tempItemTable.keySet()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                node.acquire();
                node.findItem(key);
                node.release();
            }
        }
    }
}
