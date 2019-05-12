package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class Testing {
    private static final int[] portArray ={5554,5556,5558,5560,5562};
    public static void main(String args[])
    {
        for (int i :portArray) {
            try {
                System.out.println(i + " = " + genHash(i+""));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        System.out.print("My Coordinator : " + findCoordinator("32GbSHSmgjRLq6cjBA9doo3A6haua1c2"));


    }

    private static int findCoordinator(String key)
    {
        try {
        String keyHash = genHash(key);
        System.out.println("Key Hash " + keyHash);
            if(keyHash.compareTo( genHash(String.valueOf(5562))) > 0 && keyHash.compareTo( genHash(String.valueOf(5556))) < 1)
            {
               return 5556*2;
            }
            else if(keyHash.compareTo( genHash(String.valueOf(5556))) > 0 && keyHash.compareTo( genHash(String.valueOf(5554))) < 1)
            {
                return 5554*2;
            }
            else if(keyHash.compareTo( genHash(String.valueOf(5554))) > 0 && keyHash.compareTo( genHash(String.valueOf(5558))) < 1)
            {
                return 5558*2;
            }
            else if(keyHash.compareTo( genHash(String.valueOf(5558))) > 0 && keyHash.compareTo( genHash(String.valueOf(5560))) < 1)
            {
                return 5560*2;
            }
            else
                return 5562*2;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
