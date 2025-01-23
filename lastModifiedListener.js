const axios = require("axios");
const esClient = require("./elasticsearch");
const { uploadFileToBlob } = require("./blobStorage");
const processFieldContent =
  require("./postgresqlwebhookServices").processFieldContent;
const processBlobField =
  require("./postgresqlwebhookServices").processBlobField;
const detectMimeType = require("./postgresqlwebhookServices").detectMimeType;

async function fetchIndicesWithPrefix(prefix) {
  try {
    const result = await esClient.cat.indices({ format: "json" });
    const indices = result.filter((index) => index.index.startsWith(prefix));
    return indices.map((index) => index.index);
  } catch (error) {
    console.error("Error fetching indices from Elasticsearch:", error.message);
    throw new Error("Failed to fetch indices from Elasticsearch");
  }
}

async function fetchIndexDetails(indexName) {
  try {
    const result = await esClient.search({
      index: indexName,
      body: {
        query: {
          match_all: {},
        },
      },
    });

    return result.hits.hits.map((hit) => ({
      source: hit._source,
      id: hit._id,
    }));
  } catch (error) {
    console.error(
      `Error fetching details from index ${indexName}:`,
      error.message
    );
    throw new Error("Failed to fetch index details from Elasticsearch");
  }
}

const updateTimeInElasticsearch = async (indexName, docId, updatedAt) => {
  try {
    await esClient.update({
      index: indexName,
      id: docId,
      body: {
        doc: {
          updatedAt: updatedAt, // Ensure this is ISO format
        },
      },
    });
    console.log(`Updated updatedAt for docId ${docId} in index ${indexName}`);
  } catch (error) {
    console.error(
      `Error updating updatedAt in Elasticsearch for docId ${docId}:`,
      error.message
    );
    throw new Error("Failed to update updatedAt in Elasticsearch");
  }
};

function splitLargeText(content, maxChunkSize = 30000) {
  const chunks = [];
  for (let i = 0; i < content.length; i += maxChunkSize) {
    chunks.push(content.substring(i, i + maxChunkSize));
  }
  return chunks;
}

async function fetchUpdatedRows(config) {
  const { Pool } = require("pg");

  const pool = new Pool({
    host: config.source.host,
    user: config.source.user,
    password: config.source.password,
    database: config.source.database,
    ssl: {
      rejectUnauthorized: false,
    },
  });

  const client = await pool.connect();

  try {
    console.log("Current updatedAt in Elasticsearch:", config.source.updatedAt);

    const query = `
            SELECT row_id, title AS ${config.source.title_field}, change_time, new_value AS ${config.source.field_name}, action_type,
            octet_length(new_value) AS file_size,
            CURRENT_TIMESTAMP AS uploaded_at
            FROM ${config.source.table_name}_changelog
            WHERE change_time > $1
            ORDER BY change_time ASC
        `;

    const lastIndexedTime = new Date(
      config.source.updatedAt || 0
    ).toISOString();

    const result = await client.query(query, [lastIndexedTime]);

    if (result.rows.length > 0) {
      const latestChangeTime = new Date(
        result.rows[result.rows.length - 1].change_time
      ).toISOString();

      console.log("Updating Elasticsearch with:", latestChangeTime);

      await updateTimeInElasticsearch(
        `datasource_postgresql_connection_${config.source.coid.toLowerCase()}`,
        config.id,
        latestChangeTime
      );
    }

    return result.rows;
  } catch (error) {
    console.error(
      "Error fetching updated rows from PostgreSQL:",
      error.message
    );
    throw error;
  } finally {
    client.release();
  }
}

async function processAndIndexData(
  rows,
  database_name,
  table_name,
  fieldName,
  titleField,
  category,
  indexName
) {
  const documents = [];

  for (const row of rows) {
    let processedContent;
    let fileUrl = "";
    let fileBuffer;
    const fieldValue = row[fieldName];
    // Determine if the field value is hexadecimal or plain text
    if (fieldValue.startsWith("\\x")) {
      // Hexadecimal case (first screenshot)
      fileBuffer = Buffer.from(fieldValue.replace(/\\x/g, ""), "hex");
    } else {
      // Plain text case (second screenshot)
      fileBuffer = Buffer.from(fieldValue, "utf8");
    }
    const fileName = row[titleField];

    try {
      // Detect MIME type dynamically
      const mimeType = await detectMimeType(fileBuffer);

      if (
        mimeType.startsWith("application/") ||
        mimeType === "text/html" ||
        mimeType === "text/csv" ||
        mimeType === "text/xml" ||
        mimeType === "text/plain"
      ) {
        console.log(`Detected MIME type: ${mimeType}`);
        const { extractedText } = await processBlobField(fileBuffer, mimeType);

        // Upload file to Azure Blob Storage
        fileUrl = await uploadFileToBlob(fileBuffer, fileName, mimeType);

        console.log("File URL => ", fileUrl);

        processedContent = extractedText;

        console.log("Extracted text from buffer => ", processedContent);
      } else {
        console.log("Unsupported MIME type:", mimeType);
        continue;
      }
    } catch (error) {
      console.error(
        `Error processing content for row ID ${row.row_id}:`,
        error.message
      );
      continue;
    }

    if (processedContent) {
      console.log("Row Action Type => ", row.action_type);
      console.log("Row's RowID => ", row.row_id);

      const chunks = splitLargeText(processedContent);
      chunks.forEach((chunk, index) => {
        documents.push({
          "@search.action":
            row.action_type === "INSERT" ? "upload" : "mergeOrUpload",
          id: `pg_${database_name}_${table_name}_${row.row_id}_${index}`,
          content: chunk,
          title: fileName,
          description: "No description",
          image: null,
          category: category,
          fileUrl: fileUrl,
          fileSize: (row.file_size / (1024 * 1024)).toFixed(2), // Convert to MB,
          uploadedAt: row.uploaded_at,
        });
      });
    }
  }

  if (documents.length > 0) {
    await pushToAzureSearch(documents, indexName);
    console.log(`Indexed ${documents.length} documents.`);
  } else {
    console.log("No documents to index.");
  }
}

async function pushToAzureSearch(documents, indexName) {
  try {
    const response = await axios.post(
      `${process.env.AZURE_SEARCH_ENDPOINT}/indexes/${indexName}/docs/index?api-version=2021-04-30-Preview`,
      { value: documents },
      {
        headers: {
          "Content-Type": "application/json",
          "api-key": process.env.AZURE_SEARCH_API_KEY,
        },
      }
    );

    console.log("Data pushed to Azure Search successfully.");
    return response.data;
  } catch (error) {
    console.error("Failed to push data to Azure Search:", error.message);
    throw new Error("Azure Search push failed.");
  }
}

async function processIndices(indices) {
  for (const indexName of indices) {
    try {
      const indexDetails = await fetchIndexDetails(indexName);

      for (const config of indexDetails) {
        try {
          const updatedRows = await fetchUpdatedRows(config);
          if (updatedRows.length > 0) {
            await processAndIndexData(
              updatedRows,
              config.source.database,
              config.source.table_name,
              config.source.field_name,
              config.source.title_field,
              config.source.category,
              `tenant_${config.source.coid.toLowerCase()}`
            );

            console.log("Fetched Updated Rows => ", updatedRows);
          }
        } catch (error) {
          console.error(
            `Error processing table: ${config.source.table_name}, field: ${config.source.field_name}`,
            error.message
          );
        }
      }
    } catch (error) {
      console.error(`Error processing index: ${indexName}`, error.message);
    }
  }
}

exports.lastModifiedListener = async () => {
  try {
    console.log("Fetching indices with prefix...");
    const indices = await fetchIndicesWithPrefix(
      "datasource_postgresql_connection_"
    );

    if (indices.length > 0) {
      await processIndices(indices);
    } else {
      console.log("No indices found with the specified prefix.");
    }
  } catch (error) {
    console.error("Error during periodic indexing:", error.message);
  }
};
