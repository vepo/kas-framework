package dev.vepo.kas.experiment.data.generator;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawDataSupplier implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(RawDataSupplier.class);
    private static final int MAX_IMAGE_SIZE_BYTES = 50 * 1024 * 1024; // 50MB max per image
    private static final int SELECTED_IMAGES_COUNT = 20;

    private final List<byte[]> selectedImages;
    private final AtomicInteger currentIndex;
    private volatile boolean running;

    private static Stream<File> load(Path root) {
        logger.info("Finding images... path={}", root);
        try {
            return Files.walk(root)
                        .filter(Files::isRegularFile)
                        .filter(path -> {
                            var name = path.toString().toLowerCase();
                            return name.endsWith(".jpg") ||
                                    name.endsWith(".png") ||
                                    name.endsWith(".bmp") ||
                                    name.endsWith(".jpeg");
                        })
                        .map(Path::toFile)
                        .filter(file -> file.length() <= MAX_IMAGE_SIZE_BYTES);
        } catch (IOException e) {
            logger.error("Error walking directory: %s".formatted(root), e);
            return Stream.empty();
        }
    }

    public RawDataSupplier(Path root, int messageSize) {
        this.selectedImages = new ArrayList<>();
        this.currentIndex = new AtomicInteger(0);
        this.running = true;

        // Load all valid images
        var allImages = load(root).toArray(File[]::new);
        logger.info("{} total images found!", allImages.length);

        if (allImages.length == 0) {
            logger.error("No images found in path: {}", root);
            throw new IllegalStateException("No images available to supply");
        }

        // Select 20 random images (or fewer if not enough images)
        var imagesToSelect = Math.min(SELECTED_IMAGES_COUNT, allImages.length);
        logger.info("Selecting {} random images from {} total", imagesToSelect, allImages.length);

        // Randomly select images
        var selectedFiles = new ArrayList<File>();
        var remainingFiles = new ArrayList<>(List.of(allImages));
        var random = new SecureRandom();

        for (int i = 0; i < imagesToSelect; i++) {
            var randomIndex = random.nextInt(remainingFiles.size());
            selectedFiles.add(remainingFiles.remove(randomIndex));
        }

        // Load all selected images at startup
        logger.info("Loading {} selected images into memory...", imagesToSelect);
        long totalSize = 0;

        for (int i = 0; i < selectedFiles.size(); i++) {
            File imageFile = selectedFiles.get(i);
            try {
                var imageBytes = Files.readAllBytes(imageFile.toPath());
                selectedImages.add(Arrays.copyOf(imageBytes, messageSize));
                totalSize += imageBytes.length;
                logger.debug("Loaded image {}: {} ({} bytes)", i, imageFile.getName(), imageBytes.length);
            } catch (IOException e) {
                logger.error("Failed to load selected image: {}", imageFile.getPath(), e);
                throw new RuntimeException("Failed to load image: " + imageFile.getPath(), e);
            } catch (OutOfMemoryError e) {
                logger.error("Out of memory while loading image: {}", imageFile.getPath(), e);
                throw new RuntimeException("Out of memory loading images", e);
            }
        }

        logger.info("Successfully loaded {} images, total size: {} MB",
                    selectedImages.size(), totalSize / (1024 * 1024));

        if (selectedImages.isEmpty()) {
            throw new IllegalStateException("Failed to load any images");
        }
    }

    public byte[] next() {
        if (!running) {
            throw new IllegalStateException("Supplier has been closed");
        }

        if (selectedImages.isEmpty()) {
            throw new IllegalStateException("No images available to supply");
        }

        // Get next image sequentially (circular)
        return selectedImages.get(currentIndex.getAndIncrement() % selectedImages.size());
    }

    @Override
    public void close() {
        running = false;
        selectedImages.clear();
        logger.info("RawDataSupplier closed and resources cleared");
    }
}