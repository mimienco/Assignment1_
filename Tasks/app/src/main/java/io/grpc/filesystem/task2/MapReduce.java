package io.grpc.filesystem.task2;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

public class MapReduce {

    public static String makeChunks(String inputFilePath) throws IOException {
        int count = 1;
        int size = 500;
        File f = new File(inputFilePath);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String l = br.readLine();

            while (l != null) {
                File newFile = new File(f.getParent() + "/temp", "chunk"
                        + String.format("%03d", count++) + ".txt");
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
                    int fileSize = 0;
                    while (l != null) {
                        byte[] bytes = (l + System.lineSeparator()).getBytes(Charset.defaultCharset());
                        if (fileSize + bytes.length > size)
                            break;
                        out.write(bytes);
                        fileSize += bytes.length;
                        l = br.readLine();
                    }
                }
            }
        }
        return f.getParent() + "/temp";
    }
    /**
     * @param inputfilepath
     * @throws IOException
     */
    public static void map(String inputfilepath) throws IOException {
        /*
         * Insert your code here
         * Take a chunk and filter words (you could use "\\p{Punct}" for filtering punctuations and "^[a-zA-Z0-9]"
         * together for filtering the words), then split the sentences to take out words and assign "1" as the initial count.
         * Use the given mapper class to create the unsorted key-value pair.
         * Save the map output in a file named "map-chunk001", for example, in folder
         * path input/temp/map
         */
        File filePath = new File(inputfilepath);
        File mapfolder = new File(filePath.getParentFile(), "map");

        if (!mapfolder.exists()) {
            mapfolder.mkdirs();
            System.out.println("Directory created " + mapfolder.getAbsolutePath());
        }

        System.out.println(inputfilepath);
        int filenumber = Integer.parseInt(inputfilepath.substring(16, 19));
        try {
            File newChunkFile = new File(mapfolder + "/map-chunk" + String.format("%03d", filenumber) + ".txt");
            if (!newChunkFile.exists()) {
                newChunkFile.createNewFile();
            }
            FileWriter chunkWriter = new FileWriter(newChunkFile);
            ArrayList<String> wordCounts = new ArrayList<>();
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] words = line.replaceAll("\\p{Punct}", "").split("\\s+");
                    for (String word : words) {
                        String cleanedWord = word.toLowerCase().replaceAll("[^a-zA-Z0-9]", "");
                        if (!word.isEmpty()) { // Check if the word is not empty
                            wordCounts.add(cleanedWord);

                        }
                    }
                }
            }
            for (String word : wordCounts) {
                chunkWriter.write(word + ": " + 1 + "\n");
            }
            chunkWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * @param inputfilepath
     * @param outputfilepath
     * @return
     * @throws IOException
     */
    public static void reduce(String inputfilepath, String outputfilepath) throws IOException {
        /*
         * Insert your code here
         * Take all the files in the map folder and reduce them to one file that shows
         * unique words with their counts as "the:64", for example.
         * Save the output of reduce function as output-task2.txt in the already existing output folder
         */
        Map<String, Integer> wordCountMap = new HashMap<>();

        File inputFolder = new File("input/temp/map");
        File[] mapFiles = inputFolder.listFiles((dir, name) -> name.startsWith("map-chunk"));

        if (mapFiles != null) {
            for (File mapFile : mapFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(mapFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split(":");
                        if (parts.length == 2) {
                            String word = parts[0].trim();
                            int count = Integer.parseInt(parts[1].trim());
                            wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + count);
                        }
                    }
                }
            }
        }
        wordCountMap = wordCountMap.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputfilepath))) {
            for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                writer.write(entry.getKey() + ":" + entry.getValue());
                writer.newLine();
            }
        }
    }
    /**
     * Takes a text file as an input and returns counts of each word in a text file
     * "output-task2.txt"
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException { // update the main function if required
        String inputFilePath = args[0];
        String outputFilePath = args[1];
        String chunkpath = makeChunks(inputFilePath);
        File dir = new File(chunkpath);
        File[] directoyListing = dir.listFiles();
        if (directoyListing != null) {
            for (File f : directoyListing) {
                if (f.isFile()) {
                    map(f.getPath());
                }
            }
            reduce(chunkpath, outputFilePath);
        }
    }
}
