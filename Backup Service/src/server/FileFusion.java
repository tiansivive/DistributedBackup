package server;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;


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
        System.out.println("FILE FUSION : "+fileName);
    }
    
    private String getFileName(String path) {
        String fileSeparator = System.getProperty("file.separator");
        int lastFileSeparatorIndex = path.lastIndexOf(fileSeparator);
        return path.substring(lastFileSeparatorIndex+1);
    }

    @Override
    public void run() {

        /*
        if(file.exists() && file.isDirectory()) {

            File[] files = file.listFiles();
            for(File f : files) {

                String fileIdPattern = "[a-z0-9]{64}";
                if(f.getName().matches(fileIdPattern)) {

                    int byteCounter = 0, chunkCounter = 0;
                    String pattern = "chunk_[0-9]{1,6}";
                    File[] chunks = f.listFiles();
                    
                    Arrays.sort(chunks,FileFusion.FileNameComparator);
                    for(File f2 : chunks) {
                        System.out.println(f.getName()+" : "+f2.getName());
                        if(f2.getName().matches(pattern)){
                            byteCounter += f2.length();
                            chunkCounter++;
                        } else {
                            System.out.println(f2.getName()+" is not a file chunk");
                        }
                    }
                    System.out.printf("Total bytes: %d in %d chunks", byteCounter, chunkCounter);

                    // fusion the files together
                    byte[] finalFile = new byte[byteCounter];
                    byteCounter = 0;

                    for(File f2 : chunks) {
                        byte[] fileBytes = new byte[(int)f2.length()];
                        FileInputStream in = new FileInputStream(f2);
                        if(in.read(fileBytes) != f2.length()) {
                            System.out.println("ERROR 3");
                            System.exit(-1);
                        }
                        System.arraycopy(fileBytes, 0, finalFile, byteCounter, (int)f2.length());
                        byteCounter += f2.length();
                    }
                    File output = new File(f.getName()+"."+args[1]);
                    FileOutputStream fop = new FileOutputStream(output);
                    fop.write(finalFile);
                    fop.flush();
                    fop.close();
                } else {
                    System.out.println("ERROR 2");
                }
            }
        } else {
            System.out.println("ERROR 1");
        }
        */
    }
}
