/*
 * Created on 30.07.2003
 */
package ru.myx.xstore.s2.indexing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae2.indexing.ExtractorHtmlVariant;
import ru.myx.ae2.indexing.ExtractorPlainExact;
import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae2.indexing.WeighterContrastStemmed;
import ru.myx.ae3.common.Value;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.help.Create;
import ru.myx.ae3.help.Text;
import ru.myx.ae3.indexing.ExtractorPlainVariant;
import ru.myx.ae3.report.Report;
import ru.myx.util.HashMapPrimitiveInt;

/**
 * @author myx
 */
public final class IndexingS2 {
	private final IndexingStemmer		stemmer;
	
	private final IndexingDictionary	dictionary;
	
	private final String				tnIndices;
	
	/**
	 * @param stemmer
	 * @param dictionary
	 * @param tnIndices
	 */
	public IndexingS2(final IndexingStemmer stemmer, final IndexingDictionary dictionary, final String tnIndices) {
		this.stemmer = stemmer == null
				? IndexingStemmer.DUMMY_STEMMER
				: stemmer;
		this.dictionary = dictionary == null
				? IndexingDictionary.DICT_HASHCODE
				: dictionary;
		this.tnIndices = tnIndices;
	}
	
	private final void addText(
			final HashMapPrimitiveInt<AtomicInteger> wordsExact,
			final HashMapPrimitiveInt<AtomicInteger> words,
			final String key,
			final String text,
			final int value,
			final int min,
			final Object attachment) {
		if (text == null || text.length() == 0) {
			return;
		}
		List<String> stemmedWords = null;
		final List<String> content = new ArrayList<>();
		if (words != null) {
			if (text.charAt( 0 ) == '#') {
				ExtractorPlainVariant.extractContent( content, text );
			} else {
				ExtractorHtmlVariant.extractContent( content, text );
			}
		} else {
			ExtractorPlainExact.extractContent( content, text );
		}
		final Map<String, AtomicInteger> weights = Create.tempMap();
		WeighterContrastStemmed.analyzeContent( weights, content );
		for (final Map.Entry<String, AtomicInteger> current : weights.entrySet()) {
			final String word = current.getKey();
			if (word.length() < min) {
				continue;
			}
			final int mainCode = this.dictionary.storeWordCode( Text.limitString( word, 50 )
					+ ':'
					+ Text.limitString( key, 29 ),
					true,
					attachment );
			if (mainCode != 0) {
				{
					final AtomicInteger counter = wordsExact.get( mainCode );
					if (counter == null) {
						wordsExact.put( mainCode, new AtomicInteger( value * 3 * current.getValue().intValue() ) );
					} else {
						counter.addAndGet( value * 3 * current.getValue().intValue() );
					}
				}
				if (words != null) {
					if (stemmedWords == null) {
						stemmedWords = new ArrayList<>();
					}
					this.stemmer.fillForms( word, stemmedWords );
					if (!stemmedWords.isEmpty()) {
						for (final String wrd : stemmedWords) {
							if (wrd.length() == 0) {
								continue;
							}
							final int stemmedCode = this.dictionary.storeWordCode( Text.limitString( wrd, 50 )
									+ ':'
									+ Text.limitString( key, 29 ), false, attachment );
							if (stemmedCode != 0) {
								final AtomicInteger counter = words.get( stemmedCode );
								if (counter == null) {
									words.put( stemmedCode, new AtomicInteger( value * current.getValue().intValue() ) );
								} else {
									counter.addAndGet( value * current.getValue().intValue() );
								}
							}
						}
						stemmedWords.clear();
					}
				}
			}
		}
	}
	
	private final void addTextCheck(
			final HashMapPrimitiveInt<AtomicInteger> wordsExact,
			final HashMapPrimitiveInt<AtomicInteger> words,
			final String key,
			final Object element,
			final int weight,
			final int min,
			final boolean prepare,
			final Object attachment) {
		if (element == null) {
			return;
		}
		final Object value;
		if (element instanceof Value<?>) {
			value = ((Value<?>) element).baseValue();
			if (value == null) {
				return;
			}
		} else {
			value = element;
		}
		if (value.getClass() == String.class
				|| value instanceof CharSequence
				|| value instanceof Number
				|| value instanceof Boolean) {
			this.addText( wordsExact, words, key, String.valueOf( value ), weight, min, attachment );
		} else if (value instanceof Collection<?>) {
			final Collection<?> valueCollection = (Collection<?>) value;
			if (!valueCollection.isEmpty()) {
				for (final Object element2 : valueCollection) {
					if (element2 == null) {
						continue;
					}
					this.addTextCheck( wordsExact, words, key, element2, weight, min, prepare, attachment );
				}
			}
		} else if (value instanceof Object[]) {
			final Object[] valueArray = (Object[]) value;
			if (valueArray.length > 0) {
				for (int iterator = valueArray.length - 1; iterator >= 0; iterator--) {
					final Object element2 = valueArray[iterator];
					if (element2 == null) {
						continue;
					}
					this.addTextCheck( wordsExact, words, key, element2, weight, min, prepare, attachment );
				}
			}
		}
	}
	
	private final int doCreateIndex(
			final Connection conn,
			final int luid,
			final boolean fullText,
			final Map<String, Object> fields,
			final int variant,
			final Set<String> systemKeys,
			final HashMapPrimitiveInt<AtomicInteger> map,
			final HashMapPrimitiveInt<AtomicInteger> mapExact) {
		final StringBuilder allFieldNames = new StringBuilder();
		final StringBuilder keywords = new StringBuilder();
		if (fullText) {
			keywords.append( Convert.MapEntry.toString( fields, "KEYWORDS", "" ) );
		}
		int words = 0;
		final Map<String, Object> allKeys = new TreeMap<>();
		if (systemKeys != null) {
			for (final String current : systemKeys) {
				allKeys.put( current, current );
			}
		}
		for (final String key : fields.keySet()) {
			if (key.length() == 0 || key.charAt( 0 ) == '$') {
				continue;
			}
			final String storeKey = Text.limitString( key, 29 );
			final Object names = allKeys.get( storeKey );
			if (names == null) {
				allKeys.put( storeKey, key );
			} else if (names.getClass() == String.class) {
				final Set<String> newNames = new MultipleSet();
				newNames.add( (String) names );
				newNames.add( key );
				allKeys.put( storeKey, newNames );
			} else if (names instanceof MultipleSet) {
				((MultipleSet) names).add( key );
			}
		}
		for (final Map.Entry<String, Object> current : allKeys.entrySet()) {
			final String storeKey = current.getKey();
			final Object kindaKeys = current.getValue();
			final Collection<String> keys;
			if (kindaKeys instanceof MultipleSet) {
				keys = (MultipleSet) kindaKeys;
			} else {
				keys = Collections.singleton( (String) kindaKeys );
			}
			for (final String key : keys) {
				if (key.length() == 0) {
					continue;
				}
				final Object value;
				{
					final Object valueReal = fields.get( key );
					if (valueReal == null) {
						continue;
					}
					if (valueReal instanceof Value<?>) {
						value = ((Value<?>) valueReal).baseValue();
					} else {
						value = valueReal;
					}
				}
				if (value == null) {
					continue;
				}
				if (value.getClass() == String.class || value instanceof CharSequence) {
					final String valueString = value.toString().trim();
					if (valueString.length() > 0) {
						if (key.charAt( 0 ) != '$') {
							allFieldNames.append( ' ' ).append( key );
						}
						if (!fullText) {
							keywords.append( ' ' ).append( value );
						}
						this.addText( mapExact, null, storeKey, valueString, 1, 1, conn );
					}
				} else if (value instanceof Number || value instanceof Boolean) {
					if (key.charAt( 0 ) != '$') {
						allFieldNames.append( ' ' ).append( key );
					}
					if (!fullText) {
						keywords.append( ' ' ).append( value );
					}
					this.addText( mapExact, null, storeKey, value.toString(), 1, 1, conn );
				} else if (value instanceof Collection<?>) {
					final Collection<?> valueCollection = (Collection<?>) value;
					if (!valueCollection.isEmpty()) {
						if (key.charAt( 0 ) != '$') {
							allFieldNames.append( ' ' ).append( key );
						}
						for (final Object element : valueCollection) {
							if (element == null) {
								continue;
							}
							this.addTextCheck( mapExact, null, storeKey, element, 1, 1, true, conn );
						}
					}
				} else if (value instanceof Object[]) {
					final Object[] valueArray = (Object[]) value;
					if (valueArray.length > 0) {
						if (key.charAt( 0 ) != '$') {
							allFieldNames.append( ' ' ).append( key );
						}
						for (int iterator = valueArray.length - 1; iterator >= 0; iterator--) {
							final Object element = valueArray[iterator];
							if (element == null) {
								continue;
							}
							this.addTextCheck( mapExact, null, storeKey, element, 1, 1, true, conn );
						}
					}
				} else {
					if (key.charAt( 0 ) != '$') {
						allFieldNames.append( ' ' ).append( key );
					}
				}
			}
			words += this.store( conn, mapExact, luid, variant );
			mapExact.clear();
		}
		if (keywords.length() > 0) {
			this.addText( mapExact, map, "$text", keywords.toString(), 1, 2, conn );
			words += this.store( conn, map, luid, variant );
			map.clear();
			words += this.store( conn, mapExact, luid, variant );
			mapExact.clear();
		}
		if (allFieldNames.length() > 0) {
			this.addText( map, null, "$afields", allFieldNames.toString(), 1, 1, conn );
			words += this.store( conn, map, luid, variant );
			map.clear();
		}
		return words;
	}
	
	/**
	 * @param conn
	 * @param luid
	 * @throws SQLException
	 */
	public void doDelete(final Connection conn, final int luid) throws SQLException {
		if (luid == -1) {
			return;
		}
		try (final PreparedStatement ps = conn.prepareStatement( "DELETE FROM " + this.tnIndices + " WHERE luid=?" )) {
			ps.setInt( 1, luid );
			ps.executeUpdate();
		}
	}
	
	/**
	 * @param conn
	 * @param parent
	 * @param hierarchy
	 * @param state
	 * @param systemKeys
	 * @param fields
	 * @param fullText
	 * @param luid
	 * @return int
	 * @throws SQLException
	 */
	public int doIndex(
			final Connection conn,
			final String parent,
			final Set<?> hierarchy,
			final int state,
			final Set<String> systemKeys,
			final Map<String, Object> fields,
			final boolean fullText,
			final int luid) throws SQLException {
		if (luid == -1) {
			return 0;
		}
		if (conn != null) {
			this.doDelete( conn, luid );
		} else {
			System.out.println( "IDX: DELETE FROM " + this.tnIndices + " WHERE luid=?" );
		}
		if (state == 0) {
			return 0;
		}
		final HashMapPrimitiveInt<AtomicInteger> map = new HashMapPrimitiveInt<>();
		final HashMapPrimitiveInt<AtomicInteger> mapExact = new HashMapPrimitiveInt<>();
		int words = 0;
		words += this.doCreateIndex( conn, luid, fullText, fields, 0, systemKeys, map, mapExact );
		if (parent != null) {
			this.addText( map, null, "$parent", parent, 1, 1, conn );
			words += this.store( conn, map, luid, 0 );
			map.clear();
		}
		if (hierarchy != null) {
			this.addText( map, null, "$hierarchy", hierarchy.toString(), 1, 1, conn );
			words += this.store( conn, map, luid, 0 );
			map.clear();
		}
		{
			switch (state) {
			case ModuleInterface.STATE_ARCHIEVED: {
				this.addText( map, null, "$state", "public archive archieve", 1, 1, conn );
				break;
			}
			case ModuleInterface.STATE_DEAD: {
				this.addText( map, null, "$state", "dead", 1, 1, conn );
				break;
			}
			case ModuleInterface.STATE_PUBLISHED: {
				this.addText( map, null, "$state", "public archieve listable", 1, 1, conn );
				break;
			}
			case ModuleInterface.STATE_SYSTEM: {
				this.addText( map, null, "$state", "public listable system", 1, 1, conn );
				break;
			}
			default: {
				// empty
			}
			}
			words += this.store( conn, map, luid, 0 );
			map.clear();
		}
		return words;
	}
	
	/**
	 * @return
	 */
	public IndexingDictionary getDictionary() {
		return this.dictionary;
	}
	
	/**
	 * @return
	 */
	public IndexingStemmer getStemmer() {
		return this.stemmer;
	}
	
	/**
	 * @return version
	 */
	@SuppressWarnings("static-method")
	public int getVersion() {
		return 19;
	}
	
	private final int store(
			final Connection conn,
			final HashMapPrimitiveInt<AtomicInteger> words,
			final int luid,
			final int variant) {
		final int size = words.size();
		if (size == 0) {
			return 0;
		}
		if (conn == null) {
			for (final HashMapPrimitiveInt.Entry<AtomicInteger> entry : words.entrySet()) {
				final int cd = entry.getKey();
				final int wt = entry.getValue().intValue();
				System.out.println( "IDX: INSERT INTO "
						+ this.tnIndices
						+ "(code,variant,luid,weight) VALUES ("
						+ cd
						+ ","
						+ variant
						+ ","
						+ luid
						+ ","
						+ wt
						+ ")" );
			}
			return size;
		}
		try {
			try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
					+ this.tnIndices
					+ "(code,variant,luid,weight) VALUES (?,"
					+ variant
					+ ","
					+ luid
					+ ",?)" )) {
				if (size > 1 && conn.getMetaData().supportsBatchUpdates()) {
					int counter = 0;
					for (final HashMapPrimitiveInt.Entry<AtomicInteger> entry : words.entrySet()) {
						ps.setInt( 1, entry.getKey() );
						ps.setInt( 2, entry.getValue().intValue() );
						ps.addBatch();
						if (++counter == 1024) {
							ps.executeBatch();
							counter = 0;
						}
					}
					if (counter > 0) {
						ps.executeBatch();
					}
				} else {
					for (final HashMapPrimitiveInt.Entry<AtomicInteger> entry : words.entrySet()) {
						ps.setInt( 1, entry.getKey() );
						ps.setInt( 2, entry.getValue().intValue() );
						ps.executeUpdate();
					}
				}
			}
			return size;
		} catch (final SQLException e) {
			Report.exception( "INDEXING", "Error while storing word info, intFields=true"
					+ ", luid="
					+ luid
					+ ", words<code,weight>="
					+ words
					+ ", variant="
					+ variant, e );
			return 0;
		}
	}
}
