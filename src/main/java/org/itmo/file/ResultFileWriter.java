package org.itmo.file;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ResultFileWriter {
    private final PrintWriter fileWriter;

    public ResultFileWriter(String outputFilePath) {
        this.fileWriter = initializeFileWriter(outputFilePath);
    }

    private PrintWriter initializeFileWriter(String filePath) {
        try {
            Path path = Paths.get(filePath);
            Files.createDirectories(path.getParent());

            return new PrintWriter(
                    new java.io.FileWriter(filePath, true),
                    true // autoflush
            );
        } catch (IOException e) {
            System.err.println("Error creating file writer: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public void writeToFile(String message) {
        fileWriter.println(message);
    }

    public void writeToFile(String format, Object... args) {
        writeToFile(String.format(format, args));
    }
}
