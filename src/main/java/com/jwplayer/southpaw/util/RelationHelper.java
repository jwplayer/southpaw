package com.jwplayer.southpaw.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.jwplayer.southpaw.json.Relation;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RelationHelper {
    /**
     * Used for doing object <-> JSON mappings
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Searches for the relation for the given child entity.
     * @param relation - The relation (and its children) to search
     * @param childEntity - The child entity to search for
     * @return The relation for the given child entity and it's parent, or null if it doesn't exist. Returned as a
     * Pair<Parent, Child> object. If the child entity found is the root entity, the Parent is null.
     */
    public static AbstractMap.SimpleEntry<Relation, Relation> getRelation(Relation relation, String childEntity) {
        Preconditions.checkNotNull(relation);
        if(relation.getEntity().equals(childEntity)) return new AbstractMap.SimpleEntry<>(null, relation);
        if(relation.getChildren() == null) return null;
        for(Relation child: relation.getChildren()) {
            if(child.getEntity().equals(childEntity)) return new AbstractMap.SimpleEntry<>(relation, child);
            AbstractMap.SimpleEntry<Relation, Relation> retVal = getRelation(child, childEntity);
            if(retVal != null) return retVal;
        }
        return null;
    }

    /**
     * Loads all top level relations from the given URIs. Needs to fit the relations JSON schema.
     * @param uris - The URIs to load
     * @return Top level relations from the given URIs
     * @throws IOException -
     * @throws URISyntaxException -
     */
    public static Relation[] loadRelations(List<URI> uris) throws IOException, URISyntaxException {
        List<Relation> retVal = new ArrayList<>();
        for(URI uri: uris) {
            retVal.addAll(Arrays.asList(mapper.readValue(FileHelper.loadFileAsString(uri), Relation[].class)));
        }
        return retVal.toArray(new Relation[0]);
    }

    /**
     * Validates the given child relation.
     * @param relation - The child relation to validate
     */
    protected static void validateChildRelation(Relation relation) {
        Preconditions.checkNotNull(
                relation.getEntity(),
                "A child relation must correspond to an input record"
        );
        Preconditions.checkNotNull(
                relation.getJoinKey(),
                String.format("Child relation '%s' must have a join key", relation.getEntity())
        );
        Preconditions.checkNotNull(
                relation.getParentKey(),
                String.format("Child relation '%s' must have a parent key", relation.getEntity())
        );
    }

    /**
     * Validates that the given root relations are properly constructed.
     * @param relations - The relations to validate
     */
    public static void validateRootRelations(Relation[] relations) {
        for(Relation relation: relations) {
            Preconditions.checkNotNull(
                    relation.getDenormalizedName(),
                    "A root relation must have a denormalized object name"
            );
            Preconditions.checkNotNull(
                    relation.getEntity(),
                    String.format("Top level relation '%s' must correspond to an input record type", relation.getDenormalizedName())
            );
            Preconditions.checkNotNull(
                    relation.getChildren(),
                    String.format("Top level relation '%s' must have children", relation.getDenormalizedName())
            );

            for(Relation child: relation.getChildren()) {
                validateChildRelation(child);
            }
        }
    }
}
