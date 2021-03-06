/*
 * Copyright 2018 Longtail Ad Solutions (DBA JW Player)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jwplayer.southpaw.index;

import com.jwplayer.southpaw.util.ByteArray;

import java.util.Set;

/**
 * Interface for indices that support a reverse lookup (primary key -> foreign key(s))
 */
public interface Reversible {
    /**
     * Get the foreign keys for the given primary key.
     * @param primaryKey - The primary key to lookup
     * @return The keys for the given primary key or null if no corresponding entry exists
     */
    Set<ByteArray> getForeignKeys(ByteArray primaryKey);
}
