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
package com.jwplayer.southpaw.state;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BaseStateTest {

    @Test
    public void parseAlways() {
        assertEquals(BaseState.RestoreMode.ALWAYS, BaseState.RestoreMode.parse("always"));

        assertEquals(BaseState.RestoreMode.ALWAYS, BaseState.RestoreMode.parse("ALWAYS"));

        assertEquals(BaseState.RestoreMode.ALWAYS, BaseState.RestoreMode.parse("aLwAyS"));
    }

    @Test
    public void parseWhenNeeded() {
        assertEquals(BaseState.RestoreMode.WHEN_NEEDED, BaseState.RestoreMode.parse("when_needed"));

        assertEquals(BaseState.RestoreMode.WHEN_NEEDED, BaseState.RestoreMode.parse("WHEN_NEEDED"));

        assertEquals(BaseState.RestoreMode.WHEN_NEEDED, BaseState.RestoreMode.parse("WhEn_NeEdEd"));
    }

    @Test
    public void parseNever() {
        assertEquals(BaseState.RestoreMode.NEVER, BaseState.RestoreMode.parse("never"));

        assertEquals(BaseState.RestoreMode.NEVER, BaseState.RestoreMode.parse("NEVER"));

        assertEquals(BaseState.RestoreMode.NEVER, BaseState.RestoreMode.parse("NeVeR"));
    }

    @Test
    public void parseInvalid() {
        assertNull(BaseState.RestoreMode.parse("not-a-mode"));
    }
}
