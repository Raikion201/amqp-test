package com.amqp.plugin.shovel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages all Shovels in the system.
 */
public class ShovelManager {
    private static final Logger logger = LoggerFactory.getLogger(ShovelManager.class);

    private final ConcurrentMap<String, Shovel> shovels;

    public ShovelManager() {
        this.shovels = new ConcurrentHashMap<>();
    }

    /**
     * Create and start a new shovel.
     */
    public Shovel createShovel(String name, Shovel.ShovelSource source,
                               Shovel.ShovelDestination destination, ShovelConfig config) {
        if (shovels.containsKey(name)) {
            throw new IllegalArgumentException("Shovel already exists: " + name);
        }

        Shovel shovel = new Shovel(name, source, destination, config);
        shovels.put(name, shovel);
        shovel.start();

        logger.info("Created shovel: {}", name);
        return shovel;
    }

    /**
     * Get a shovel by name.
     */
    public Shovel getShovel(String name) {
        return shovels.get(name);
    }

    /**
     * Delete a shovel.
     */
    public void deleteShovel(String name) {
        Shovel shovel = shovels.remove(name);
        if (shovel != null) {
            shovel.stop();
            logger.info("Deleted shovel: {}", name);
        }
    }

    /**
     * Get all shovels.
     */
    public List<Shovel> getAllShovels() {
        return new ArrayList<>(shovels.values());
    }

    /**
     * Get statistics for all shovels.
     */
    public List<Shovel.ShovelStats> getAllStats() {
        List<Shovel.ShovelStats> stats = new ArrayList<>();
        for (Shovel shovel : shovels.values()) {
            stats.add(shovel.getStats());
        }
        return stats;
    }

    /**
     * Stop all shovels.
     */
    public void stopAll() {
        logger.info("Stopping all shovels");
        for (Shovel shovel : shovels.values()) {
            shovel.stop();
        }
    }

    /**
     * Get shovel count.
     */
    public int getShovelCount() {
        return shovels.size();
    }
}
