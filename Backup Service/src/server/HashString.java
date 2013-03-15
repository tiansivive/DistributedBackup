package server;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashString {
    
    static public String getFileIdentifier(File f) {
        
        String fileIdentifierString = new String(f.getName() + f.length() + f.getParent() + f.lastModified());        
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        md.update(fileIdentifierString.getBytes());
        byte byteData[] = md.digest();

        //convert the byte to hex format
        StringBuffer hexString = new StringBuffer();
        for (int i=0; i<byteData.length; i++) {
            String hex = Integer.toHexString(0xff & byteData[i]);
            if(hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
