/*******************************************************************************
 * Copyright 2017 Google Inc.
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
 *******************************************************************************/
package com.google.cloud.dataflow.examples.opinionanalysis;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipeline.FilterProcessedUrls;
import com.google.cloud.dataflow.examples.opinionanalysis.IndexerPipeline.GetUrlFn;
import com.google.cloud.dataflow.examples.opinionanalysis.model.InputContent;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.Read;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.ArrayUtils;

import sirocco.util.IdConverterUtils;

import java.util.HashMap;
import java.util.Map;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 */
public class IndexerPipelineUtils {

	public static final String WEBRESOURCE_TABLE = "webresource";
	public static final String WRSOCIALCOUNT_TABLE = "wrsocialcount";
	public static final String DOCUMENT_TABLE = "document";
	public static final String SENTIMENT_TABLE = "sentiment";
	public static final String STATTOPIC_TABLE = "stattopic";
	public static final String STATNGRAM_TABLE = "statngram";
	

	// IDs of known document collections
	public static final String DOC_COL_ID_KGA = "01";
	public static final String DOC_COL_ID_REDDIT_FH_BIGQUERY = "02";
	public static final String DOC_COL_ID_GDELT_BUCKET = "03";
	public static final String DOC_COL_ID_CSV_FILE = "04";
	

	// Reddit domain url
	public static final String REDDIT_URL = "https://www.reddit.com";

	public static final DateTimeFormatter dateTimeFormatYMD_HMS = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss")
			.withZoneUTC();

	public static final DateTimeFormatter dateTimeFormatYMD_HMS_MSTZ = DateTimeFormat
			.forPattern("yyyy-MM-dd HH:mm:ss.SSS z");

	public static final DateTimeFormatter dateTimeFormatYMD = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();

	public static final DateTimeFormatter dateTimeFormatYMD_T_HMS_Z = DateTimeFormat
			.forPattern("yyyyMMdd'T'HHmmss'Z'");
	
	// Bigtable Dead Letter table and columns
	public static final String DEAD_LETTER_TABLE = "unprocessed-documents";
	public static final String DEAD_LETTER_TABLE_ERR_CF = "err";
	
	// Integration with Cloud NLP
	public static final String CNLP_TAG_PREFIX = "cnlp::";
	
	// Metafields constants
	public static final int METAFIELDS_REDDIT_NUM_FIELDS = 5;
	public static final int METAFIELDS_REDDIT_EXTERNALLINK_IDX = 0;
	public static final int METAFIELDS_REDDIT_SUBREDDIT_IDX = 1;
	public static final int METAFIELDS_REDDIT_SCORE_IDX = 2;
	public static final int METAFIELDS_REDDIT_POSTID_IDX = 3;
	public static final int METAFIELDS_REDDIT_DOMAIN_IDX = 4;
	
	
	public static final String METAFIELDS_VALUE_UNVAILABLE = "unavailable";
	
	// TODO: Move the date parsing functions to DateUtils
	public static DateTime parseDateString(DateTimeFormatter formatter, String s) {
		DateTime result = null;
		try {
			result = formatter.parseDateTime(s);
		} catch (Exception e) {
			result = null;
		}
		return result;
	}

	public static Long parseDateToLong(DateTimeFormatter formatter, String s) {
		Long result = null;
		DateTime date = parseDateString(formatter, s);
		if (date != null)
			result = date.getMillis();
		return result;
	}

	public static Long parseDateToLong(String s) {
		Long pt = parseDateToLong(dateTimeFormatYMD_HMS, s);
		if (pt == null)
			pt = parseDateToLong(dateTimeFormatYMD, s);
		return pt;
	}

	// same as in sirocco.utils.IdConverterUtils
	public static int getDateIdFromTimestamp(long millis) {
		int result;
		DateTime dt = new DateTime(millis, DateTimeZone.UTC);
		int year = dt.getYear();
		int month = dt.getMonthOfYear();
		int day = dt.getDayOfMonth();
		result = day + (100 * month) + (10000 * year);
		return result;
	}

	public static Integer parseDateToInteger(String s) {
		Integer result = null;
		Long l = parseDateToLong(s);
		if (l != null) {
			result = getDateIdFromTimestamp(l.longValue());
		}
		return result;
	}

	public static void setTableRowFieldIfNotNull(TableRow r, String field, Object value) {
		if (value != null)
			r.set(field, value);
	}

	public static String getTableRowStringFieldIfNotNull(TableRow r, String field) {
		Object value = r.get(field);
		if (value != null)
			return value.toString();
		else
			return null;
	}

	
	public static String buildJdbcSourceImportQuery(IndexerPipelineOptions options) {
		String timeWindow = null;
		if (!(options.getJdbcSourceFromDate() == null || options.getJdbcSourceFromDate().isEmpty()))
			timeWindow = "	AND pages.pub_at >= '" + options.getJdbcSourceFromDate() + "'";
		if (!(options.getJdbcSourceToDate() == null || options.getJdbcSourceToDate().isEmpty())) {
			if (timeWindow != null)
				timeWindow += " AND ";
			else
				timeWindow = "";
			timeWindow += "pages.pub_at <= '" + options.getJdbcSourceToDate() + "'";
		}
		if (timeWindow == null && options.getJdbcSourceTimeWindowSec() != null)
			timeWindow = "	AND pages.pub_at >= DATE_SUB(NOW(), INTERVAL " + options.getJdbcSourceTimeWindowSec()
					+ " SECOND)";

		String result = "SELECT\n" + "	pages.url AS url,\n" + "	unix_timestamp(pages.pub_at) AS pub_time,\n"
				+ "	pages.title AS title,\n" + "	pages.author AS author,\n" + "	'en' AS language,\n"
				+ "	page_texts.text AS page_text,\n" + "	'" + DOC_COL_ID_KGA + "' AS doc_col_id,\n"
				+ "	IFNULL(pages.duplicate_page_id, pages.id) AS col_item_id,\n"
				+ "	IF(pages.duplicate_page_id IS NOT NULL,1,0) AS skip_indexing\n" + "FROM pages\n"
				+ "	LEFT JOIN page_texts on pages.id = page_texts.page_id\n" + "WHERE\n" + "	pages.language_id = 1\n"
				+ "	AND page_texts.text IS NOT NULL\n" + timeWindow;

		return result;

	}

	public static String buildJdbcSourceImportQueryForSocialStats(IndexerPipelineOptions options) {
		String timeWindow = null;
		if (!(options.getJdbcSourceFromDate() == null || options.getJdbcSourceFromDate().isEmpty()))
			timeWindow = "	AND s.created_at >= '" + options.getJdbcSourceFromDate() + "'";
		if (!(options.getJdbcSourceToDate() == null || options.getJdbcSourceToDate().isEmpty())) {
			if (timeWindow != null)
				timeWindow += " AND ";
			else
				timeWindow = "";
			timeWindow += "s.created_at <= '" + options.getJdbcSourceToDate() + "'";
		}
		if (timeWindow == null && options.getJdbcSourceTimeWindowSec() != null)
			timeWindow = "	AND s.created_at >= DATE_SUB(NOW(), INTERVAL " + options.getJdbcSourceTimeWindowSec()
					+ " SECOND)";

		String result =

				"SELECT\n" + "	page_pub_time,\n" + "	url,\n" + "	doc_col_id,\n" + "	col_item_id,\n"
						+ "	MAX(count_time) AS count_time,\n" + "	count_tw,\n" + "	count_fb\n" + "FROM (\n"
						+ "	SELECT\n" + "		unix_timestamp(p.pub_at) AS page_pub_time,\n" + "		p.url,\n"
						+ "		'" + DOC_COL_ID_KGA + "' AS doc_col_id,\n" + "		page_id AS col_item_id,\n"
						+ "		unix_timestamp(s.created_at) AS count_time,\n"
						+ "		SUM( CASE s.provider_id WHEN 1 THEN s.count ELSE 0 END ) AS count_tw,\n"
						+ "		SUM( CASE s.provider_id WHEN 2 THEN s.count ELSE 0 END ) AS count_fb\n"
						+ "	FROM social_stats s\n" + "		INNER JOIN pages p ON p.id = s.page_id\n" + "	WHERE\n"
						+ "		s.count > 0\n" + timeWindow + "	GROUP BY 1,2,3,4,5 ) AS a1\n" + "GROUP BY 1,2,3,4,6,7\n"
						+ "ORDER BY 1,2,3,4,6,7\n";

		return result;

	}

	public static String buildBigQueryProcessedUrlsQuery(IndexerPipelineOptions options) {
		String timeWindow = null;

		if (options.getProcessedUrlHistorySec() != null) {
			if (options.getProcessedUrlHistorySec() != Integer.MAX_VALUE) {
				Instant fromTime = Instant.now();
				fromTime = fromTime.minus(options.getProcessedUrlHistorySec() * 1000L);
				Integer fromDateId = IdConverterUtils.getDateIdFromTimestamp(fromTime.getMillis());
				timeWindow = "PublicationDateId >= " + fromDateId;
			}
		}

		if (timeWindow != null)
			timeWindow = "WHERE " + timeWindow;

		String result = "SELECT Url, MAX(ProcessingTime) AS ProcessingTime\n" + "FROM " + options.getBigQueryDataset()
				+ "." + WEBRESOURCE_TABLE + "\n" + timeWindow + "\n" + "GROUP BY Url";

		return result;
	}

	public static String buildBigQueryProcessedDocsQuery(IndexerPipelineOptions options) {
		String timeWindow = null;

		if (options.getProcessedUrlHistorySec() != null) {
			if (options.getProcessedUrlHistorySec() != Integer.MAX_VALUE) {
				Instant fromTime = Instant.now();
				fromTime = fromTime.minus(options.getProcessedUrlHistorySec() * 1000L);
				Integer fromDateId = IdConverterUtils.getDateIdFromTimestamp(fromTime.getMillis());
				timeWindow = "PublicationDateId >= " + fromDateId;
			}
		}

		if (timeWindow != null)
			timeWindow = "WHERE " + timeWindow;

		String result = "SELECT DocumentHash, MAX(ProcessingTime) AS ProcessingTime\n" + "FROM " + options.getBigQueryDataset()
				+ "." + DOCUMENT_TABLE + "\n" + timeWindow + "\n" + "GROUP BY DocumentHash";

		return result;
	}

	public static String buildBigQueryProcessedSocialCountsQuery(IndexerPipelineOptions options) {
		String timeWindow = null;

		if (options.getWrSocialCountHistoryWindowSec() != null) {
			if (options.getWrSocialCountHistoryWindowSec() != Integer.MAX_VALUE) {
				Instant fromTime = Instant.now();
				fromTime = fromTime.minus(options.getWrSocialCountHistoryWindowSec() * 1000L);
				Integer fromDateId = IdConverterUtils.getDateIdFromTimestamp(fromTime.getMillis());
				timeWindow = "WrPublicationDateId >= " + fromDateId;
			}
		}

		if (timeWindow != null)
			timeWindow = "WHERE " + timeWindow;

		String result = "SELECT WebResourceHash, MAX(CountTime) AS LastCountTime\n" + "FROM "
				+ options.getBigQueryDataset() + "." + WRSOCIALCOUNT_TABLE + "\n" + timeWindow + "\n"
				+ "GROUP BY WebResourceHash";

		return result;

	}

	public static void validateIndexerPipelineOptions(IndexerPipelineOptions options) throws Exception {

		int numSourceTypes = 0;
		if (options.isSourcePubsub())
			numSourceTypes++;
		if (options.isSourceJDBC())
			numSourceTypes++;
		if (options.isSourceRecordFile())
			numSourceTypes++;
		if (options.isSourceRedditBQ())
			numSourceTypes++;
		if (options.isSourceGDELTbucket())
			numSourceTypes++;
		

		if (numSourceTypes == 0)
			throw new IllegalArgumentException("At least one of the Source Type options needs to be specified.");

		if (numSourceTypes > 1)
			throw new IllegalArgumentException("At most one of the Source Type options can to be specified.");

		if (options.isSourcePubsub()) {
			if (options.getPubsubTopic().isEmpty())
				throw new IllegalArgumentException(
						"Pub/Sub topic needs to be specified when Pub/Sub is being used as a source type.");
		}

		if (options.isSourceJDBC()) {
			if (options.getJdbcDriverClassName().isEmpty() || options.getJdbcSourceUrl().isEmpty()
					|| options.getJdbcSourceUsername().isEmpty() || options.getJdbcSourcePassword().isEmpty())
				throw new IllegalArgumentException(
						"JDBC driver class, Url, username, and password need to be specified when JDBC is being used as a source type.");

			if (options.getJdbcSourceTimeWindowSec() == null
					&& (options.getJdbcSourceFromDate() == null || options.getJdbcSourceFromDate().isEmpty())
					&& (options.getJdbcSourceToDate() == null || options.getJdbcSourceToDate().isEmpty()))
				throw new IllegalArgumentException(
						"If Source is JDBC, one of TimeWindowSec or FromDate or ToDate parameters needs to be set.");

			if (!(options.getJdbcSourceFromDate() == null || options.getJdbcSourceFromDate().isEmpty())) {
				Long l = IndexerPipelineUtils.parseDateToLong(options.getJdbcSourceFromDate());
				if (l == null)
					throw new IllegalArgumentException(
							"Invalid value of the FromDate parameter: " + options.getJdbcSourceFromDate());
			}

			if (!(options.getJdbcSourceToDate() == null || options.getJdbcSourceToDate().isEmpty())) {
				Long l = IndexerPipelineUtils.parseDateToLong(options.getJdbcSourceToDate());
				if (l == null)
					throw new IllegalArgumentException(
							"Invalid value of the ToDate parameter: " + options.getJdbcSourceToDate());
			}

		}

		if (options.isSourceRecordFile()) {
			if (options.getInputFile()==null || options.getInputFile().isEmpty())
				throw new IllegalArgumentException(
						"Input file path pattern needs to be specified when GCS is being used as a source type.");
			try {
				if (!(options.getRecordDelimiters()==null || options.getRecordDelimiters().isEmpty()))
					extractRecordDelimiters(options.getRecordDelimiters());
				
			} catch(Exception e) {
				throw new IllegalArgumentException(
						"Invalid value of the RecordDelimiters parameter: " + e.getMessage());
			}
			if (options.getReadAsCSV()!=null && options.getReadAsPropertyBag() != null && options.getReadAsCSV() && options.getReadAsPropertyBag()) {
				throw new IllegalArgumentException(
						"Only one of readAsCSV and readAsPropertBag options can be set.");
			}
			if (options.getReadAsCSV()==null || ! options.getReadAsCSV())
				options.setReadAsPropertyBag(true);
		}

		if (options.isSourceRedditBQ()) {
			if (options.getRedditPostsTableName() == null && options.getRedditPostsQuery() == null)
				throw new IllegalArgumentException(
						"If Source is Reddit, one of PostsTableName or PostsQuery parameters needs to be set.");
			if (options.getRedditCommentsTableName() == null && options.getRedditCommentsQuery() == null)
				throw new IllegalArgumentException(
						"If Source is Reddit, one of CommentsTableName or CommentsQuery parameters needs to be set.");
		}

		if (options.isSourceGDELTbucket()) {
			if (options.getInputFile().isEmpty())
				throw new IllegalArgumentException(
						"Input file path pattern needs to be specified when reading from GDELT bucket.");
		}

		if (options.getDedupeText() == null) {
			options.setDedupeText(options.isSourceGDELTbucket());
		}
		
		if ( (options.getBigQueryDataset() == null) && (options.getOutputFile() == null)) {
			throw new IllegalArgumentException("Either Sink BigQuery dataset or Output file need to be specified.");
		}

		if (options.isSourcePubsub()) {
			options.setStreaming(true);
		} else {
			options.setStreaming(false);
		}

		if (options.getRatioEnrichWithCNLP() == null) {
			Float cnlpRatio = options.isStreaming() ? 1.0F : 0.01F;
			options.setRatioEnrichWithCNLP(cnlpRatio);
		}

	}

	public static void validateSocialStatsPipelineOptions(IndexerPipelineOptions options) throws Exception {

		if (options.getJdbcDriverClassName().isEmpty() || options.getJdbcSourceUrl().isEmpty()
				|| options.getJdbcSourceUsername().isEmpty() || options.getJdbcSourcePassword().isEmpty())
			throw new IllegalArgumentException(
					"JDBC driver class, Url, username, and password need to be specified when JDBC is being used as a source type.");

		if (options.getJdbcSourceTimeWindowSec() == null
				&& (options.getJdbcSourceFromDate() == null || options.getJdbcSourceFromDate().isEmpty())
				&& (options.getJdbcSourceToDate() == null || options.getJdbcSourceToDate().isEmpty()))
			throw new IllegalArgumentException(
					"If Source is JDBC, one of TimeWindowSec or FromDate or ToDate parameters needs to be set.");

		if (!(options.getJdbcSourceFromDate() == null || options.getJdbcSourceFromDate().isEmpty())) {
			Long l = IndexerPipelineUtils.parseDateToLong(options.getJdbcSourceFromDate());
			if (l == null)
				throw new IllegalArgumentException(
						"Invalid value of the FromDate parameter: " + options.getJdbcSourceFromDate());
		}

		if (!(options.getJdbcSourceToDate() == null || options.getJdbcSourceToDate().isEmpty())) {
			Long l = IndexerPipelineUtils.parseDateToLong(options.getJdbcSourceToDate());
			if (l == null)
				throw new IllegalArgumentException(
						"Invalid value of the ToDate parameter: " + options.getJdbcSourceToDate());
		}

		if (options.getBigQueryDataset().isEmpty()) {
			throw new IllegalArgumentException("Sink BigQuery dataset needs to be specified.");
		}

		//setupBigQuery(options);

	}


	
	public static String buildRedditPostUrl(String permalink) {
		return REDDIT_URL + permalink;
	}

	public static String buildRedditCommentUrl(String postPermalink, String commentId) {
		return REDDIT_URL + postPermalink + commentId + "/";
	}

	public static Long extractRedditTime(String createdUtcString) {
		Integer i = Integer.decode(createdUtcString);
		return (i * 1000L);
	}

	public static String[] extractRedditPostMetaFields(TableRow post) {
		String[] result = new String[METAFIELDS_REDDIT_NUM_FIELDS];
		
		String domain = post.get("domain").toString();
		if (!domain.startsWith("self.")) {
			result[METAFIELDS_REDDIT_EXTERNALLINK_IDX] = post.get("url").toString();
			result[METAFIELDS_REDDIT_DOMAIN_IDX] = domain;
		} else {
			result[METAFIELDS_REDDIT_EXTERNALLINK_IDX] = METAFIELDS_VALUE_UNVAILABLE;
			result[METAFIELDS_REDDIT_DOMAIN_IDX] = METAFIELDS_VALUE_UNVAILABLE;
		}
		Object oSubreddit = post.get("subreddit");
		if (oSubreddit != null)
			result[METAFIELDS_REDDIT_SUBREDDIT_IDX] = oSubreddit.toString();
		else 
			result[METAFIELDS_REDDIT_SUBREDDIT_IDX] = METAFIELDS_VALUE_UNVAILABLE;
			
		result[METAFIELDS_REDDIT_SCORE_IDX] = post.get("score").toString();
		result[METAFIELDS_REDDIT_POSTID_IDX] = extractPostIdFromRedditPost(post);
		
		return result;
	}
	
	public static String[] extractRedditCommentMetaFields(TableRow comment) {
		
		String[] result = new String[METAFIELDS_REDDIT_NUM_FIELDS];
		
		result[METAFIELDS_REDDIT_EXTERNALLINK_IDX] = METAFIELDS_VALUE_UNVAILABLE;
		result[METAFIELDS_REDDIT_DOMAIN_IDX] = METAFIELDS_VALUE_UNVAILABLE;
		
		Object oSubreddit = comment.get("subreddit");
		if (oSubreddit != null)
			result[METAFIELDS_REDDIT_SUBREDDIT_IDX] = oSubreddit.toString();
		else
			result[METAFIELDS_REDDIT_SUBREDDIT_IDX] = METAFIELDS_VALUE_UNVAILABLE;
		
		result[METAFIELDS_REDDIT_SCORE_IDX] = comment.get("score").toString();
		result[METAFIELDS_REDDIT_POSTID_IDX] = extractPostIdFromRedditComment(comment);
		
		return result;
		
	}
	
	public static String extractPostIdFromRedditPost(TableRow post) {
		return "t3_" + (String) post.get("id");
	}

	public static String extractPostIdFromRedditComment(TableRow comment) {
		return (String) comment.get("link_id");
		// link_id in comments is already in the format t3_<postId>
	}
	
	public static byte[] extractRecordDelimiters(String paramValue) {
		String[] delimiters = paramValue.split(",");
		ArrayList<Byte> resList = new ArrayList<Byte>();
		for (String d: delimiters)
			resList.add(Byte.parseByte(d));
		Byte[] resArray = new Byte[resList.size()];
		resList.toArray(resArray);
		byte[] result = ArrayUtils.toPrimitive(resArray);
		return result;
	}
	/**
	 * Extracts the post id from the post record.
	 */
	static class ExtractPostDataFn extends DoFn<TableRow, KV<String, TableRow>> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String postId = IndexerPipelineUtils.extractPostIdFromRedditPost(row);
			c.output(KV.of(postId, row));
		}
	}

	/**
	 * Extracts the post id from the comment record.
	 */
	static class ExtractCommentInfoFn extends DoFn<TableRow, KV<String, TableRow>> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String postId = extractPostIdFromRedditComment(row);
			c.output(KV.of(postId, row));
		}
	}

	

	
}
