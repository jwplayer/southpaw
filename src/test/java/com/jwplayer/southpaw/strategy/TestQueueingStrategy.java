package com.jwplayer.southpaw.strategy;

import com.jwplayer.southpaw.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

public class TestQueueingStrategy extends QueueingStrategy {
    private static final Logger logger =  LoggerFactory.getLogger(TestQueueingStrategy.class);

    @Override
    public Priority getPriority(String denormalizedName, String entity, Set<ByteArray> primaryKeys) {
        Priority priority = Priority.MEDIUM;
        if(denormalizedName.equals("DenormalizedPlaylist")) {
            switch (entity) {
                case "media":
                    priority = Priority.HIGH;
                    break;
                case "user_tag":
                    priority = Priority.NONE;
                    break;
                case "playlist_custom_params":
                    priority = Priority.LOW;
                    break;
                default:
                    priority = Priority.MEDIUM;
            }
        }
        logger.info(String.format("%d %s priority PKs for: %s / %s / %s",
                primaryKeys.size(),
                priority,
                denormalizedName,
                entity,
                primaryKeys.stream().map(Object::toString).collect(Collectors.joining(", "))));
        return priority;
    }
}
