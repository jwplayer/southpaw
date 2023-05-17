package com.jwplayer.southpaw.util;

import com.jwplayer.southpaw.json.Relation;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.AbstractMap;
import java.util.Collections;

import static org.junit.Assert.*;

public class RelationHelperTest {
    private static final String BROKEN_RELATIONS_PATH = "test-resources/broken_relations.sample.json";
    private static final String RELATIONS_PATH = "test-resources/relations.sample.json";

    private URI brokenRelationsUri;
    private URI relationsUri;

    @Before
    public void setup() throws Exception {
        brokenRelationsUri = new URI(BROKEN_RELATIONS_PATH);
        relationsUri = new URI(RELATIONS_PATH);
    }

    @Test
    public void testGetRelationChild() throws Exception {
        Relation relation = RelationHelper.loadRelations(Collections.singletonList(relationsUri))[0];
        AbstractMap.SimpleEntry<Relation, Relation> foundRelation = RelationHelper.getRelation(relation, "media");

        // Parent
        assertEquals("playlist_media", foundRelation.getKey().getEntity());
        assertEquals("playlist_id", foundRelation.getKey().getJoinKey());
        assertEquals("id", foundRelation.getKey().getParentKey());
        assertEquals(1, foundRelation.getKey().getChildren().size());
        // Child
        assertEquals("media", foundRelation.getValue().getEntity());
        assertEquals("id", foundRelation.getValue().getJoinKey());
        assertEquals("media_id", foundRelation.getValue().getParentKey());
        assertEquals(0, foundRelation.getValue().getChildren().size());
    }

    @Test
    public void testGetRelationMissing() throws Exception {
        Relation relation = RelationHelper.loadRelations(Collections.singletonList(relationsUri))[0];
        AbstractMap.SimpleEntry<Relation, Relation> foundRelation = RelationHelper.getRelation(relation, "your mom");

        assertNull(foundRelation);
    }

    @Test
    public void testGetRelationRoot() throws Exception {
        Relation relation = RelationHelper.loadRelations(Collections.singletonList(relationsUri))[0];
        AbstractMap.SimpleEntry<Relation, Relation> foundRelation = RelationHelper.getRelation(relation, relation.getEntity());

        assertNull(foundRelation.getKey());
        assertEquals(relation, foundRelation.getValue());
    }

    @Test
    public void testLoadRelations() throws Exception {
        Relation[] relations = RelationHelper.loadRelations(Collections.singletonList(relationsUri));

        assertEquals(1, relations.length);
        assertEquals("DenormalizedPlaylist", relations[0].getDenormalizedName());
        assertEquals("playlist", relations[0].getEntity());
        assertEquals(4, relations[0].getChildren().size());
    }

    @Test
    public void testValidateRootRelations() throws Exception {
        Relation[] relations = RelationHelper.loadRelations(Collections.singletonList(relationsUri));
        RelationHelper.validateRootRelations(relations);
    }

    @Test(expected = NullPointerException.class)
    public void testValidateRootRelationsException() throws Exception {
        Relation[] relations = RelationHelper.loadRelations(Collections.singletonList(brokenRelationsUri));
        RelationHelper.validateRootRelations(relations);
    }
}
