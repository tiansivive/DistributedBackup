package server;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import constantValues.Values;


public class FileFusion implements Runnable {
    
    private File file;
    private String fileName;
    
    
    public static Comparator<File> FileNameComparator = new Comparator<File>() {

        public int compare(File f1, File f2) {

            int number1 = Integer.parseInt(f1.getName().substring(6)); 
            int number2 = Integer.parseInt(f2.getName().substring(6)); 

            //ascending order
            return number1 - number2;
        }
    };
    
    public FileFusion(File fileDirectory, String path) {
        file = fileDirectory;
        fileName = getFileName(path);
    }
    
    private String getFileName(String path) {
        String fileSeparator = System.getProperty("file.separator");
        int lastFileSeparatorIndex = path.lastIndexOf(fileSeparator);
        return Values.directory_to_restore_files+fileSeparator+path.substring(lastFileSeparatorIndex+1);
    }

    @Override
    public void run() {

        
        File finalFile = new File(fileName);
        
        if(finalFile.exists()) {
            System.out.println("FILE ALREADY IS RESTORED!");
            System.exit(-1);
        }
 
        try {
            if(finalFile.createNewFile()) { // was able to create it
 
                FileOutputStream fop = new FileOutputStream(finalFile);
 
                if(file.exists() && file.isDirectory()) {
                    File[] chunks = file.listFiles();
                    Arrays.sort(chunks,FileFusion.FileNameComparator);
 
                    int byteCounter = 0, chunkCounter = 0;
                    String pattern = "chunk_[0-9]{1,6}";
 
                    for(File f : chunks) {
                        if(f.getName().matches(pattern)){
                            System.out.println(f.getName()+" CHUNK");
                            FileInputStream in = new FileInputStream(f);
                            byte[] fileBytes = new byte[(int)f.length()];
                            if(in.read(fileBytes) != f.length()) {
                                System.out.println("ERROR 3");
                                System.exit(-1);
                            }
                            fop.write(fileBytes);
                            fop.flush();
                            byteCounter += f.length();
                            chunkCounter++;
                            f.delete();
                        } else {
                            System.out.println(f.getName()+" is not a file chunk");
                        }
                    }
                    System.out.printf("Total bytes: %d in %d chunks\n", byteCounter, chunkCounter);
                    fop.close();
                    file.delete();
                } else {
                    System.out.println("ERROR 2");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }   
    }
}
