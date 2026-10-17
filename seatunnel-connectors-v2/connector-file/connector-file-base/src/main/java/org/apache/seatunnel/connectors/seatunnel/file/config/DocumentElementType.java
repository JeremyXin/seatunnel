/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.config;

/** Element types emitted by the Markdown and PDF document readers. */
public enum DocumentElementType {
    HEADING("heading"),
    PARAGRAPH("paragraph"),
    IMAGE("image"),
    LINK("link"),
    BULLET_LIST("bullet_list"),
    ORDERED_LIST("ordered_list"),
    BULLET_LIST_ITEM("bullet_list_item"),
    ORDERED_LIST_ITEM("ordered_list_item"),
    BLOCK_QUOTE("block_quote"),
    FENCED_CODE_BLOCK("fenced_code_block"),
    TABLE_BLOCK("table_block");

    private final String value;

    DocumentElementType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
