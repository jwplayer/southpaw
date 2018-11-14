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
package com.jwplayer.southpaw.filter;

import com.jwplayer.southpaw.record.BaseRecord;

public class TestFilter extends BaseFilter {
    String DELETED = "deleted";
    String INVALID = "INVALID";

    public TestFilter() {}

    @Override
    public FilterMode filter(String entity, BaseRecord record, BaseRecord oldRecord) {
        FilterMode mode = FilterMode.UPDATE;
        switch(entity) {
            case "media":
                if (DELETED.equals(record.get("status"))) {
                    mode = FilterMode.DELETE;
                }
                break;
            case "playlist_custom_params":
                if (INVALID.equals(record.get("value"))) {
                    mode = FilterMode.DELETE;
                }
                break;
            default:
                mode = FilterMode.UPDATE;
        }
        return mode;
    }
}
