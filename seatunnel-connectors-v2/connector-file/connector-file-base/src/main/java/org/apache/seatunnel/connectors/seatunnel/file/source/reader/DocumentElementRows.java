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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.DocumentElement;
import org.apache.seatunnel.connectors.seatunnel.file.source.FileSourceDocumentRouting;

import java.util.List;

/** Maps parsed document elements to the common Markdown and PDF source schema. */
final class DocumentElementRows {
    private static final String[] FIELD_NAMES = {
        "element_id",
        "element_type",
        "heading_level",
        "text",
        "page_number",
        "position_index",
        "parent_id",
        "child_ids"
    };
    private static final SeaTunnelDataType[] FIELD_TYPES = {
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.INT_TYPE,
        BasicType.STRING_TYPE,
        BasicType.INT_TYPE,
        BasicType.INT_TYPE,
        BasicType.STRING_TYPE,
        ArrayType.STRING_ARRAY_TYPE
    };
    private static final String[] RAG_FIELD_NAMES = {
        "source_uri", "document_id", "chunk_id", "chunk_index", "content_hash"
    };
    private static final SeaTunnelDataType[] RAG_FIELD_TYPES = {
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.STRING_TYPE,
        BasicType.INT_TYPE,
        BasicType.STRING_TYPE
    };

    private final String sourceUri;
    private final String documentId;
    private final boolean ragEnabled;

    DocumentElementRows(String sourcePath, boolean ragEnabled) {
        this.sourceUri = FileSourceDocumentRouting.normalizeSourceUri(sourcePath);
        this.documentId = FileSourceDocumentRouting.buildDocumentId(sourceUri);
        this.ragEnabled = ragEnabled;
    }

    static SeaTunnelRowType rowType(boolean ragEnabled) {
        if (!ragEnabled) {
            return new SeaTunnelRowType(FIELD_NAMES, FIELD_TYPES);
        }
        return new SeaTunnelRowType(
                concat(FIELD_NAMES, RAG_FIELD_NAMES), concat(FIELD_TYPES, RAG_FIELD_TYPES));
    }

    static void assignPositionIndices(List<DocumentElement> elements) {
        for (int i = 0; i < elements.size(); i++) {
            elements.get(i).setPositionIndex(i);
        }
    }

    SeaTunnelRow toRow(DocumentElement element, int chunkIndex) {
        SeaTunnelRow base = element.toSeaTunnelRow();
        if (!ragEnabled) {
            return base;
        }
        Object[] fields = new Object[FIELD_NAMES.length + RAG_FIELD_NAMES.length];
        for (int i = 0; i < FIELD_NAMES.length; i++) {
            fields[i] = base.getField(i);
        }
        String contentHash = contentHash(element.getText());
        fields[8] = sourceUri;
        fields[9] = documentId;
        fields[10] =
                "chunk_"
                        + FileSourceDocumentRouting.sha256Hex(
                                documentId + ":" + chunkIndex + ":" + contentHash);
        fields[11] = chunkIndex;
        fields[12] = contentHash;
        return new SeaTunnelRow(fields);
    }

    static String contentHash(String text) {
        return FileSourceDocumentRouting.sha256Hex(text == null ? "" : text);
    }

    private static String[] concat(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static SeaTunnelDataType[] concat(SeaTunnelDataType[] left, SeaTunnelDataType[] right) {
        SeaTunnelDataType[] result = new SeaTunnelDataType[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }
}
