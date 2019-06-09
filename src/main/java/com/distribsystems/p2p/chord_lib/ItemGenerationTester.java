package com.distribsystems.p2p.chord_lib;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class ItemGenerationTester extends Thread {
    private final static boolean GENERATION_TESTER_ENABLE = false;
    private final static int ITEM_TESTER_PORT_NUMBER = 8004;
    private Map<BigInteger, String> tempItemTable = new HashMap<>();
    private int portNumber;
    private Node node;

    /**
     * @brief   Fill the ItemTable with Fake Items just for Test purposes
     * @param   node
     */
    public ItemGenerationTester(Node node) {
        this.portNumber = node.getPort();
        this.node       = node;

        //Only the node with port number chosen does this thing
        if(this.portNumber == ITEM_TESTER_PORT_NUMBER && GENERATION_TESTER_ENABLE) {
            tempItemTable.put(new BigInteger("12"), "Item no. 12");
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
            tempItemTable.put(new BigInteger("27"), "Item no. 27");
            tempItemTable.put(new BigInteger("29"), "Item no. 29");
            tempItemTable.put(new BigInteger("98"), "Item no. 98");
            tempItemTable.put(new BigInteger("180"), "Item no. 180");
            tempItemTable.put(new BigInteger("182"), "Item no. 182");
            tempItemTable.put(new BigInteger("190"), "Item no. 190");
            tempItemTable.put(new BigInteger("205"), "Item no. 205");
            tempItemTable.put(new BigInteger("203"), "Item no. 203");
            tempItemTable.put(new BigInteger("191"), "Item no. 191");
            tempItemTable.put(new BigInteger("192"), "Item no. 192");
        }
    }

    @Override
    public void run() {
        //Only the node with port number chosen does this thing
        if(this.portNumber == ITEM_TESTER_PORT_NUMBER && GENERATION_TESTER_ENABLE) {
            String response = Chord.NOT_FOUND;

            //Wait 30seconds before starting
            try {
                Thread.sleep(20000);
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
                node.placeItem(key, tempItemTable.get(key));
                node.release();
            }
        }
    }
}
