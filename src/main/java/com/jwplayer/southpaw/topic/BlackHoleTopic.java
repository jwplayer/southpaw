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
package com.jwplayer.southpaw.topic;

import com.jwplayer.southpaw.util.ByteArray;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.commons.lang.NotImplementedException;

import java.util.Iterator;


public class BlackHoleTopic<K, V> extends BaseTopic<K, V> {
    @Override
    public void commit() {
        // noop
    }

    @Override
    public void flush() {
        // noop
    }

    @Override
    public Map<Integer, Long> getCurrentOffsets() {
        throw new NotImplementedException();
    }

    @Override
    public long getLag() {
        throw new NotImplementedException();
    }

    @Override
    public V readByPK(ByteArray primaryKey) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> readNext() {
        throw new NotImplementedException();
    }

    @Override
    public void resetCurrentOffsets() {
        throw new NotImplementedException();
    }

    @Override
    public void write(K key, V value) {
        // noop
    }
}
