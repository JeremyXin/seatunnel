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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.KnowledgeSyncMetadataField;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.DocumentElement;
import org.apache.seatunnel.connectors.seatunnel.file.config.DocumentElementType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.MarkdownKnowledgeSyncMetadata;

import org.apache.commons.io.IOUtils;

import com.vladsch.flexmark.ast.BlockQuote;
import com.vladsch.flexmark.ast.BulletList;
import com.vladsch.flexmark.ast.Code;
import com.vladsch.flexmark.ast.FencedCodeBlock;
import com.vladsch.flexmark.ast.Heading;
import com.vladsch.flexmark.ast.Image;
import com.vladsch.flexmark.ast.Link;
import com.vladsch.flexmark.ast.ListItem;
import com.vladsch.flexmark.ast.OrderedList;
import com.vladsch.flexmark.ast.Paragraph;
import com.vladsch.flexmark.ast.ThematicBreak;
import com.vladsch.flexmark.ext.tables.TableBlock;
import com.vladsch.flexmark.ext.tables.TableCell;
import com.vladsch.flexmark.ext.tables.TableRow;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Node;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class MarkdownReadStrategy extends AbstractReadStrategy {

    private static final int DEFAULT_PAGE_NUMBER = 1;
    private static final int DEFAULT_POSITION = 1;

    private boolean markdownRagMetadataEnabled =
            FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.defaultValue();

    private static final class NodeInfo {
        private final String elementId;
        private final String parentId;
        private final List<String> childIds = new ArrayList<>();
        private final int positionIndex;

        private NodeInfo(String elementId, String parentId, int positionIndex) {
            this.elementId = elementId;
            this.parentId = parentId;
            this.positionIndex = positionIndex;
        }
    }

    /** Per-file logical identity and digest reused by every row emitted from that document. */
    private static final class LogicalDocumentMetadata {
        private final String sourceUri;
        private final String documentId;
        private final String documentHash;

        private LogicalDocumentMetadata(String sourceUri, String documentId, String documentHash) {
            this.sourceUri = sourceUri;
            this.documentId = documentId;
            this.documentHash = documentHash;
        }
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        String logicalSourceUri = null;
        String logicalDocumentId = null;
        MessageDigest documentDigest = null;
        if (markdownRagMetadataEnabled) {
            logicalSourceUri = MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(path);
            logicalDocumentId = MarkdownKnowledgeSyncMetadata.buildDocumentId(logicalSourceUri);
            documentDigest = MarkdownKnowledgeSyncMetadata.newSha256Digest();
        }

        String markdown;
        try (InputStream inputStream = hadoopFileSystemProxy.getInputStream(path)) {
            InputStream contentStream = inputStream;
            if (documentDigest != null) {
                contentStream = new DigestInputStream(inputStream, documentDigest);
            }
            markdown = IOUtils.toString(contentStream, StandardCharsets.UTF_8);
        }
        LogicalDocumentMetadata logicalMetadata =
                documentDigest == null
                        ? null
                        : new LogicalDocumentMetadata(
                                logicalSourceUri,
                                logicalDocumentId,
                                MarkdownKnowledgeSyncMetadata.toLowerHex(documentDigest.digest()));
        collectMarkdownRows(markdown, path, logicalMetadata, output);
    }

    /**
     * Emits rows from converted Markdown while preserving the identity of the original source.
     *
     * <p>{@code sourcePath} and {@code sourceDocumentHash} must describe the original source file,
     * not an intermediate Markdown file or the converted Markdown bytes.
     */
    void collectMarkdownRows(
            String markdown,
            String sourcePath,
            String sourceDocumentHash,
            Collector<SeaTunnelRow> output) {
        validateConvertedMarkdown(markdown, sourcePath, sourceDocumentHash);
        LogicalDocumentMetadata logicalMetadata = null;
        if (markdownRagMetadataEnabled) {
            String logicalSourceUri =
                    MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(sourcePath);
            logicalMetadata =
                    new LogicalDocumentMetadata(
                            logicalSourceUri,
                            MarkdownKnowledgeSyncMetadata.buildDocumentId(logicalSourceUri),
                            sourceDocumentHash);
        }
        collectMarkdownRows(markdown, sourcePath, logicalMetadata, output);
    }

    private void validateConvertedMarkdown(
            String markdown, String sourcePath, String sourceDocumentHash) {
        if (sourcePath == null || sourcePath.trim().isEmpty()) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.DATA_DESERIALIZE_FAILED,
                    "Cannot parse converted Markdown without the original source path");
        }
        if (markdown == null || markdown.trim().isEmpty()) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.DATA_DESERIALIZE_FAILED,
                    "Converted Markdown is empty for source: " + sourcePath);
        }
        if (markdownRagMetadataEnabled
                && (sourceDocumentHash == null || sourceDocumentHash.trim().isEmpty())) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.DATA_DESERIALIZE_FAILED,
                    "Original document hash is missing for source: " + sourcePath);
        }
    }

    private void collectMarkdownRows(
            String markdown,
            String sourcePath,
            LogicalDocumentMetadata logicalMetadata,
            Collector<SeaTunnelRow> output) {
        List<DocumentElement> elements = parseMarkdownElements(markdown);
        DocumentElementRows rowMapper =
                new DocumentElementRows(sourcePath, markdownRagMetadataEnabled);
        for (int i = 0; i < elements.size(); i++) {
            DocumentElement element = elements.get(i);
            SeaTunnelRow row = rowMapper.toRow(element, i + 1);
            if (logicalMetadata != null) {
                addKnowledgeSyncMetadata(
                        row,
                        logicalMetadata.sourceUri,
                        logicalMetadata.documentId,
                        logicalMetadata.documentHash,
                        DocumentElementRows.contentHash(element.getText()));
            }
            output.collect(row);
        }
    }

    /** Parses Markdown content into document elements. */
    List<DocumentElement> parseMarkdownElements(String markdown) {
        Node document = Parser.builder().build().parse(markdown == null ? "" : markdown);
        Map<Node, NodeInfo> nodeInfoMap = new IdentityHashMap<>();
        Map<String, Integer> typeCounters = new HashMap<>();
        Map<Node, DocumentElement> elementsByNode = new IdentityHashMap<>();
        List<DocumentElement> elements = new ArrayList<>();
        assignIdsAndCollectTree(document, null, nodeInfoMap, DEFAULT_POSITION, typeCounters);
        generateElements(document, elements, nodeInfoMap, elementsByNode);
        assignHeadingParents(document, elementsByNode);
        return elements;
    }

    private void assignIdsAndCollectTree(
            Node node,
            Node parent,
            Map<Node, NodeInfo> nodeInfoMap,
            int position,
            Map<String, Integer> typeCounters) {
        String elementType = node.getClass().getSimpleName();
        String elementId = null;

        if (isEligibleForRow(node)) {
            int count = typeCounters.getOrDefault(elementType, 0) + 1;
            typeCounters.put(elementType, count);
            elementId = elementType + "_" + count;
        }

        String parentId = parent == null ? null : nodeInfoMap.get(parent).elementId;
        NodeInfo nodeInfo = new NodeInfo(elementId, parentId, position);
        nodeInfoMap.put(node, nodeInfo);

        int childPosition = 1;
        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            assignIdsAndCollectTree(child, node, nodeInfoMap, childPosition++, typeCounters);
            NodeInfo childInfo = nodeInfoMap.get(child);
            if (childInfo.elementId != null) {
                nodeInfo.childIds.add(childInfo.elementId);
            }
        }
    }

    private void generateElements(
            Node node,
            List<DocumentElement> elements,
            Map<Node, NodeInfo> nodeInfoMap,
            Map<Node, DocumentElement> elementsByNode) {
        if (isEligibleForRow(node)) {
            NodeInfo nodeInfo = nodeInfoMap.get(node);
            DocumentElement element = new DocumentElement(elementType(node), extractValue(node));
            element.setElementId(nodeInfo.elementId);
            if (node instanceof Heading) {
                element.setHeadingLevel(((Heading) node).getLevel());
            }
            element.setParentId(nodeInfo.parentId);
            element.setChildIds(new ArrayList<>(nodeInfo.childIds));
            element.setPageNumber(DEFAULT_PAGE_NUMBER);
            element.setPositionIndex(nodeInfo.positionIndex);
            elements.add(element);
            elementsByNode.put(node, element);
        }
        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            generateElements(child, elements, nodeInfoMap, elementsByNode);
        }
    }

    private void assignHeadingParents(Node document, Map<Node, DocumentElement> elementsByNode) {
        Deque<DocumentElement> headings = new ArrayDeque<>();
        for (Node node = document.getFirstChild(); node != null; node = node.getNext()) {
            DocumentElement element = elementsByNode.get(node);
            if (element == null) {
                continue;
            }
            if (node instanceof Heading) {
                while (!headings.isEmpty()
                        && headings.peek().getHeadingLevel() >= element.getHeadingLevel()) {
                    headings.pop();
                }
            }
            if (!headings.isEmpty()) {
                element.setParentId(headings.peek().getElementId());
                headings.peek().addChildId(element.getElementId());
            }
            if (node instanceof Heading) {
                headings.push(element);
            }
        }
    }

    private DocumentElementType elementType(Node node) {
        if (node instanceof Heading) {
            return DocumentElementType.HEADING;
        }
        if (node instanceof Paragraph) {
            return DocumentElementType.PARAGRAPH;
        }
        if (node instanceof OrderedList) {
            return DocumentElementType.ORDERED_LIST;
        }
        if (node instanceof BulletList) {
            return DocumentElementType.BULLET_LIST;
        }
        if (node instanceof ListItem) {
            return node.getParent() instanceof OrderedList
                    ? DocumentElementType.ORDERED_LIST_ITEM
                    : DocumentElementType.BULLET_LIST_ITEM;
        }
        if (node instanceof BlockQuote) {
            return DocumentElementType.BLOCK_QUOTE;
        }
        if (node instanceof FencedCodeBlock) {
            return DocumentElementType.FENCED_CODE_BLOCK;
        }
        if (node instanceof TableBlock) {
            return DocumentElementType.TABLE_BLOCK;
        }
        throw new IllegalArgumentException("Unsupported Markdown element: " + node.getClass());
    }

    private boolean isEligibleForRow(Node node) {
        if (node instanceof Paragraph) {
            Node parent = node.getParent();
            if (parent instanceof ListItem || parent instanceof BlockQuote) {
                return false;
            }
        }

        return node instanceof Heading
                || node instanceof Paragraph
                || node instanceof ListItem
                || node instanceof BulletList
                || node instanceof OrderedList
                || node instanceof BlockQuote
                || node instanceof FencedCodeBlock
                || node instanceof TableBlock;
    }

    private String extractValue(Node node) {
        if (node instanceof ListItem) {
            return extractTextFromChildren(node);
        } else if (node instanceof Heading || node instanceof Paragraph) {
            return extractTextFromChildren(node);
        } else if (node instanceof BulletList) {
            return bulletListToString((BulletList) node);
        } else if (node instanceof OrderedList) {
            return orderedListToString((OrderedList) node);
        } else if (node instanceof Code) {
            return ((Code) node).getText().toString();
        } else if (node instanceof FencedCodeBlock) {
            return ((FencedCodeBlock) node).getContentChars().toString();
        } else if (node instanceof BlockQuote) {
            return extractTextFromChildren(node);
        } else if (node instanceof ThematicBreak) {
            return "---";
        } else if (node instanceof Link) {
            return ((Link) node).getUrl().toString();
        } else if (node instanceof Image) {
            return ((Image) node).getUrl().toString();
        } else if (node instanceof TableBlock) {
            return tableToString((TableBlock) node);
        }

        return node.getChars().toString();
    }

    private String extractTextFromChildren(Node node) {
        StringBuilder sb = new StringBuilder();
        for (Node child = node.getFirstChild(); child != null; child = child.getNext()) {
            sb.append(child.getChars());
        }

        return sb.toString().trim();
    }

    private String bulletListToString(BulletList list) {
        StringBuilder sb = new StringBuilder();
        for (Node item = list.getFirstChild(); item != null; item = item.getNext()) {
            if (item instanceof ListItem) {
                sb.append("- ").append(extractTextFromChildren(item)).append("\n");
            }
        }

        return sb.toString();
    }

    private String orderedListToString(OrderedList list) {
        StringBuilder sb = new StringBuilder();
        int num = 1;
        for (Node item = list.getFirstChild(); item != null; item = item.getNext()) {
            if (item instanceof ListItem) {
                sb.append(num++).append(". ").append(extractTextFromChildren(item)).append("\n");
            }
        }

        return sb.toString();
    }

    private String tableToString(TableBlock table) {
        StringBuilder sb = new StringBuilder();
        for (Node row = table.getFirstChild(); row != null; row = row.getNext()) {
            if (row instanceof TableRow) {
                for (Node cell = row.getFirstChild(); cell != null; cell = cell.getNext()) {
                    if (cell instanceof TableCell) {
                        sb.append(((TableCell) cell).getText().toString()).append(" | ");
                    }
                }
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        return DocumentElementRows.rowType(markdownRagMetadataEnabled);
    }

    @Override
    public void setPluginConfig(Config pluginConfig) {
        super.setPluginConfig(pluginConfig);
        if (pluginConfig.hasPath(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key())) {
            markdownRagMetadataEnabled =
                    pluginConfig.getBoolean(
                            FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key());
        }
    }

    /** Adds the four logical Knowledge Sync fields to row options for Metadata projection. */
    static void addKnowledgeSyncMetadata(
            SeaTunnelRow row,
            String sourceUri,
            String documentId,
            String documentHash,
            String chunkHash) {
        Map<String, Object> options = row.getOptions();
        options.put(KnowledgeSyncMetadataField.SOURCE_URI.getName(), sourceUri);
        options.put(KnowledgeSyncMetadataField.DOCUMENT_ID.getName(), documentId);
        options.put(KnowledgeSyncMetadataField.DOCUMENT_HASH.getName(), documentHash);
        options.put(KnowledgeSyncMetadataField.CHUNK_HASH.getName(), chunkHash);
    }
}
