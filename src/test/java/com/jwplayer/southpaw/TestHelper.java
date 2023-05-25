package com.jwplayer.southpaw;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jwplayer.southpaw.index.Index;
import com.jwplayer.southpaw.index.Indices;
import com.jwplayer.southpaw.json.DenormalizedRecord;
import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.record.MapRecord;
import com.jwplayer.southpaw.serde.JsonSerde;
import com.jwplayer.southpaw.util.ByteArray;
import com.jwplayer.southpaw.util.ByteArraySet;
import com.jwplayer.southpaw.util.FileHelper;
import com.jwplayer.southpaw.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TestHelper {
    public static final String BASE_RESOURCE_PATH = "test-resources/";
    public static final String CONFIG_PATH = BASE_RESOURCE_PATH + "config.sample.yaml";
    public static final String INDEX_BASE_PATH = BASE_RESOURCE_PATH + "index/";

    public interface PLAYLIST_JOIN_INDICES {
        String MEDIA = "JK|media|id";
        String PLAYLIST_CUSTOM_PARAMS = "JK|playlist_custom_params|playlist_id";
        String PLAYLIST_MEDIA = "JK|playlist_media|playlist_id";
        String PLAYLIST_TAG = "JK|playlist_tag|playlist_id";
        String USER = "JK|user|user_id";
        String USER_TAG = "JK|user_tag|id";
    }
    public interface PLAYLIST_PARENT_INDICES {
        String PLAYLIST_MEDIA = "PaK|playlist|playlist_media|media_id";
        String PLAYLIST = "PaK|playlist|playlist|id";
        String PLAYLIST_USER = "PaK|playlist|playlist|user_id";
        String PLAYLIST_TAG = "PaK|playlist|playlist_tag|user_tag_id";
    }
    public interface RELATION_PATHS {
        String PLAYLIST = BASE_RESOURCE_PATH + "relations.sample.json";
        String PLAYER = BASE_RESOURCE_PATH + "relations2.sample.json";
        String MEDIA = BASE_RESOURCE_PATH + "relations3.sample.json";
    }
    public interface TOPIC_DATA_PATHS {
        String BASE_TOPIC_PATH = BASE_RESOURCE_PATH + "topic/";
        String DENORMALIZED_MEDIA = BASE_TOPIC_PATH + "DenormalizedMedia.json";
        String DENORMALIZED_PLAYER = BASE_TOPIC_PATH + "DenormalizedPlayer.json";
        String DENORMALIZED_PLAYLIST = BASE_TOPIC_PATH + "DenormalizedPlaylist.json";
        String MEDIA = BASE_TOPIC_PATH + "media.json";
        String PLAYER = BASE_TOPIC_PATH + "player.json";
        String PLAYLIST = BASE_TOPIC_PATH + "playlist.json";
        String PLAYLIST_CUSTOM_PARAMS = BASE_TOPIC_PATH + "playlist_custom_params.json";
        String PLAYLIST_MEDIA = BASE_TOPIC_PATH + "playlist_media.json";
        String PLAYLIST_TAG = BASE_TOPIC_PATH + "playlist_tag.json";
        String USER = BASE_TOPIC_PATH + "user.json";
        String USER_TAG = BASE_TOPIC_PATH + "user_tag.json";
    }

    @SuppressWarnings("unchecked")
    public static ByteArray convertPrimaryKey(Object primaryKey) {
        if(primaryKey instanceof Map) {
            return ByteArray.toByteArray(new MapRecord((Map<String, ?>) primaryKey));
        } else {
            return ByteArray.toByteArray(primaryKey);
        }
    }

    public static String getIndexDataPath(String indexName) {
        return INDEX_BASE_PATH + indexName.replace(Indices.SEP, ".") + ".json";
    }

    public static void populateIndex(Index index) throws Exception {
        List<Pair<BaseRecord, BaseRecord>> data = readRecordData(getIndexDataPath(index.getIndexName()));
        for(Pair<BaseRecord, BaseRecord> datum: data) {
            for(Object pk: (List<?>) datum.getB().get("pks")) {
                index.add(ByteArray.toByteArray(datum.getA()), convertPrimaryKey(pk));
            }
        }
        verifyIndexState(index);
    }

    public static List<Pair<ByteArray, DenormalizedRecord>> readDenormalizedData(String dataPath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        final Logger logger =  LoggerFactory.getLogger(TestHelper.class);
        List<Pair<ByteArray, DenormalizedRecord>> data = new ArrayList<>();
        String[] json = FileHelper.loadFileAsString(new URI(dataPath)).split("\n");
        for (int i = 0; i < json.length / 2; i++) {
            ByteArray key = ByteArray.toByteArray(Integer.parseInt(json[2 * i]));
            DenormalizedRecord value = null;
            if(!"null".equals(json[2 * i + 1])) value = mapper.readValue(json[2 * i + 1], DenormalizedRecord.class);
            logger.error(String.format("%s / %s / %s", dataPath, key, value));
            data.add(new Pair<>(key, value));
        }
        return data;
    }

    public static List<Pair<BaseRecord, BaseRecord>> readRecordData(String dataPath) throws Exception {
        try(JsonSerde.JsonDeserializer deserializer = new JsonSerde.JsonDeserializer()) {
            deserializer.configure(new HashMap<>(), false);
            List<Pair<BaseRecord, BaseRecord>> data = new ArrayList<>();
            String[] json = FileHelper.loadFileAsString(new URI(dataPath)).split("\n");
            for (int i = 0; i < json.length / 2; i++) {
                BaseRecord key = deserializer.deserialize(null, json[2 * i].getBytes());
                BaseRecord value = deserializer.deserialize(null, json[2 * i + 1].getBytes());
                data.add(new Pair<>(key, value.isEmpty() ? null: value));
            }
            return data;
        }
    }

    @SafeVarargs
    public static <T> ByteArraySet toSet(T... items) {
        ByteArraySet set = new ByteArraySet();
        for(T item: items) {
            set.add(ByteArray.toByteArray(item));
        }
        return set;
    }

    public static void verifyIndexState(Index index) {
        assertEquals(Collections.emptySet(), index.verifyIndexState());
        assertEquals(Collections.emptySet(), index.verifyReverseIndexState());
    }
}
