package server;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

        if(file.exists() && file.isDirectory()) {

            File[] chunks = file.listFiles();
            Arrays.sort(chunks,FileFusion.FileNameComparator);

            int byteCounter = 0, chunkCounter = 0;
            String pattern = "chunk_[0-9]{1,6}";

            for(File f : chunks) {
                if(f.getName().matches(pattern)) {
                    byteCounter += f.length();
                    chunkCounter++;
                } else {
                    System.out.println(f.getName()+" is not a file chunk");
                }
            }

            System.out.printf("Total bytes: %d in %d chunks", byteCounter, chunkCounter);

            // fusion the files together
            byte[] finalFile = new byte[byteCounter];
            byteCounter = 0;

            try {
                for(File f : chunks) {
                    byte[] fileBytes = new byte[(int)f.length()];
                    FileInputStream in = new FileInputStream(f);
                    if(in.read(fileBytes) != f.length()) {
                        System.out.println("ERROR 3");
                        System.exit(-1);
                    }
                    System.arraycopy(fileBytes, 0, finalFile, byteCounter, (int)f.length());
                    byteCounter += f.length();
                }
                File output = new File(fileName);
                FileOutputStream fop = new FileOutputStream(output);
                fop.write(finalFile);
                fop.flush();
                fop.close();
            } catch (IOException e) {
                // NEEDS TO REQUEST THE SAME BACKUP AND TRY AGAIN NO?
            }
        }
    }
}
