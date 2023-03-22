/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae2.indexing.ExecSearchCalendarProgram;
import ru.myx.ae2.indexing.ExecSearchProgram;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.reflect.Reflect;
import ru.myx.query.OneCondition;
import ru.myx.query.OneSort;
import ru.myx.query.SyntaxQuery;

/** @author myx */
final class TreeJdbc {
	
	private static final Map<String, String> REPLACEMENT_SEARCH_SORT = TreeJdbc.createSearchReplacementSort();

	private static final Map<String, String> REPLACEMENT_FIELDS_NS = TreeJdbc.createReplacementFields(0);

	private static final Map<String, Function<OneCondition, OneCondition>> REPLACEMENT_FILTER_CONDITIONS = TreeJdbc.createReplacementFilterConditions();

	static final Comparator<TreeJdbcEntry> COMPARATOR_LOG = new ComparatorLog();

	static final Comparator<TreeJdbcEntry> COMPARATOR_HISTORY = new ComparatorHistory();

	private static final String[] STRING_ARRAY_EMPTY = new String[0];

	private static final TreeJdbcEntry[] ENTRY_ARRAY_EMPTY = new TreeJdbcEntry[0];

	private static final Map<String, String> EMPTY_NAMES = Collections.emptyMap();

	static final TreeJdbc EMPTY = new TreeJdbc(null, null, -1, TreeJdbc.EMPTY_NAMES, new TreeJdbcEntry[0]);

	private static final Map<Integer, Map<Integer, Map<String, Number>>> EMPTY_MAP_CALENDAR = new TreeMap<>();

	private static final Map<String, Number> EMPTY_MAP_ALPHABET = new TreeMap<>();

	private static final String CDATE_DESC = " ORDER BY o.objCreated DESC";

	private static final String CDATE_ASC = " ORDER BY o.objCreated ASC";

	private static final String TITLE_ASC_NS = " ORDER BY o.objTitle ASC";

	private static final String TITLE_ASC_VS = " ORDER BY v.objTitle ASC";

	private static final Map<String, String> createReplacementFields(final int variant) {
		
		final Map<String, String> result = new HashMap<>();
		result.put("$guid", "t.lnkId");
		result.put("$state", "o.objState");
		result.put(
				"$title",
				variant == 0
					? "o.objTitle"
					: "v.objTitle");
		result.put("$key", "t.lnkName");
		result.put("$type", "o.objType");
		result.put("$created", "o.objCreated");
		result.put("$modified", "o.objDate");
		result.put("$folder", "t.lnkFolder");
		return result;
	}

	static final Map<String, Function<OneCondition, OneCondition>> createReplacementFilterConditions() {
		
		final Map<String, Function<OneCondition, OneCondition>> result = new HashMap<>();
		result.put("t.lnkFolder", new ReplacerConditionBooleanToChar());
		result.put("o.objState", new ReplacerConditionState());
		return result;
	}

	private static final Map<String, String> createSearchReplacementSort() {
		
		final Map<String, String> result = new HashMap<>();
		result.put("alphabet", "$title");
		result.put("history", "$created-");
		result.put("log", "$created");
		return result;
	}

	static void fillAlphabet(final Map<String, Number> map, final Map<String, String> alphabetConversion, final String defaultLetter, final String title) {
		
		final String letter = TreeJdbc.getLetter(alphabetConversion, defaultLetter, title);
		final Number value = map.get(letter);
		if (value == null) {
			map.put(letter, new AtomicInteger(1));
		} else {
			((AtomicInteger) value).incrementAndGet();
		}
	}

	static void fillStats(final Map<Integer, Map<Integer, Map<String, Number>>> map, final Calendar calendar, final Date date, final String paramName) {
		
		calendar.setTime(date);
		final Integer yearDate = Reflect.getInteger(calendar.get(Calendar.YEAR));
		final Integer monthDate = Reflect.getInteger(calendar.get(Calendar.MONTH));
		final String dayDate = "created" + calendar.get(Calendar.DAY_OF_MONTH);
		Map<Integer, Map<String, Number>> yearMap = map.get(yearDate);
		if (yearMap == null) {
			map.put(yearDate, yearMap = new TreeMap<>());
		}
		Map<String, Number> monthMap = yearMap.get(monthDate);
		if (monthMap == null) {
			yearMap.put(monthDate, monthMap = new TreeMap<>());
		}
		{
			final Object value = monthMap.get(paramName);
			if (value == null) {
				monthMap.put(paramName, new AtomicInteger(1));
			} else {
				((AtomicInteger) value).incrementAndGet();
			}
		}
		{
			final Number value = monthMap.get(dayDate);
			if (value == null) {
				monthMap.put(dayDate, new AtomicInteger(1));
			} else {
				((AtomicInteger) value).incrementAndGet();
			}
		}
	}

	private static final String getLetter(final Map<String, String> alphabetConversion, final String defaultLetter, final String title) {
		
		if (title == null) {
			return defaultLetter;
		}
		for (int i = 0; i < title.length(); ++i) {
			final char c = title.charAt(i);
			if (Character.isLetterOrDigit(c)) {
				return Convert.MapEntry.toString(alphabetConversion, String.valueOf(c), defaultLetter);
			}
		}
		return defaultLetter;
	}

	private static final String getListingSort(final BaseEntry<?> entry, final String specifiedSort) {
		
		final String sort;
		if (specifiedSort == null) {
			if (entry == null) {
				return null;
			}
			final String entrySort = entry.getEntryBehaviorListingSort();
			sort = entrySort == null
				? ""
				: entrySort.trim();
		} else {
			sort = specifiedSort.trim();
		}
		if (sort.length() == 0) {
			return null;
		}
		if (sort.length() == 0 || "history".equals(sort)) {
			return TreeJdbc.CDATE_DESC;
		}
		if ("alphabet".equals(sort)) {
			return TreeJdbc.TITLE_ASC_NS;
		}
		if ("log".equals(sort)) {
			return TreeJdbc.CDATE_ASC;
		}
		if ("list".equals(sort)) {
			return null;
		}
		final List<Object> list = new ArrayList<>();
		final List<Object> listSort = SyntaxQuery.parseOrder(list, null, TreeJdbc.REPLACEMENT_FIELDS_NS, sort);
		if (listSort == null || listSort.isEmpty()) {
			return TreeJdbc.CDATE_DESC;
		}
		final OneSort oneSort = (OneSort) listSort.get(0);
		return " ORDER BY " + oneSort.getField() + (oneSort.isDescending()
			? " DESC"
			: " ASC");
	}

	private static final String[] internLimit(final int count, final String[] base) {
		
		if (count > 0 && count < base.length) {
			final String[] result = new String[count];
			System.arraycopy(base, 0, result, 0, count);
			return result;
		}
		return base;
	}

	private static final String[] searchLocalJava(final boolean all, final long startDate, final long endDate, final TreeJdbcEntry[] entries) {
		
		final long compareEndDate = endDate == -1L
			? Long.MAX_VALUE
			: endDate;
		final List<String> result = new ArrayList<>();
		final int entryCount = entries.length;
		if (all) {
			for (int i = 0; i < entryCount; ++i) {
				final TreeJdbcEntry current = entries[i];
				final long created = current.created;
				if (created >= startDate && created < compareEndDate) {
					result.add(current.lnkId);
				}
			}
		} else {
			for (int i = 0; i < entryCount; ++i) {
				final TreeJdbcEntry current = entries[i];
				if (current.searchable) {
					final long created = current.created;
					if (created >= startDate && created < compareEndDate) {
						result.add(current.lnkId);
					}
				}
			}
		}
		return result.isEmpty()
			? TreeJdbc.STRING_ARRAY_EMPTY
			: (String[]) result.toArray(new String[result.size()]);
	}

	private final ServerJdbc server;

	private final String guid;

	private final int luid;

	private final Map<String, String> names;

	private final TreeJdbcEntry[] entries;

	private TreeJdbcEntry[] entriesLog;

	private TreeJdbcEntry[] entriesHistory;

	private String[] contents = null;

	private String[] contentsListable = null;

	private String[] contentsListableHistory = null;

	private String[] contentsListableLog = null;

	private String[] contentsSearchable = null;

	private String[] files = null;

	private String[] filesListable = null;

	private String[] folders = null;

	private String[] foldersListable = null;

	private Map<Integer, Map<Integer, Map<String, Number>>> calendarAll = null;

	private Map<Integer, Map<Integer, Map<String, Number>>> calendarSearchable = null;

	private Map<String, Number> alphabetAll = null;

	private Map<String, Number> alphabetSearchable = null;

	TreeJdbc(final ServerJdbc server, final String guid, final int luid, final Map<String, String> names, final TreeJdbcEntry[] entries) {
		
		this.server = server;
		this.guid = guid;
		this.luid = luid;
		this.names = names;
		this.entries = entries;
		if (entries == null || entries.length == 0) {
			this.entriesLog = TreeJdbc.ENTRY_ARRAY_EMPTY;
			this.entriesHistory = TreeJdbc.ENTRY_ARRAY_EMPTY;
			this.contents = TreeJdbc.STRING_ARRAY_EMPTY;
			this.contentsListable = TreeJdbc.STRING_ARRAY_EMPTY;
			this.contentsListableHistory = TreeJdbc.STRING_ARRAY_EMPTY;
			this.contentsListableLog = TreeJdbc.STRING_ARRAY_EMPTY;
			this.contentsSearchable = TreeJdbc.STRING_ARRAY_EMPTY;
			this.files = TreeJdbc.STRING_ARRAY_EMPTY;
			this.filesListable = TreeJdbc.STRING_ARRAY_EMPTY;
			this.folders = TreeJdbc.STRING_ARRAY_EMPTY;
			this.foldersListable = TreeJdbc.STRING_ARRAY_EMPTY;
			this.calendarAll = TreeJdbc.EMPTY_MAP_CALENDAR;
			this.calendarSearchable = TreeJdbc.EMPTY_MAP_CALENDAR;
			this.alphabetAll = TreeJdbc.EMPTY_MAP_ALPHABET;
			this.alphabetSearchable = TreeJdbc.EMPTY_MAP_ALPHABET;
		}
	}

	final String getChildByName(final String name) {
		
		return this.names.get(name);
	}

	final String[] getChildren(final BaseEntry<?> entry, final int count, final String sort) {
		
		if (sort == null || sort.isBlank() || "alphabet".equals(sort)) {
			if (this.contents == null) {
				synchronized (this.entries) {
					if (this.contents == null) {
						final List<String> contents = new ArrayList<>();
						final TreeJdbcEntry[] entries = this.entries;
						final int entryCount = entries.length;
						for (int i = 0; i < entryCount; ++i) {
							final TreeJdbcEntry current = entries[i];
							contents.add(current.lnkId);
						}
						this.contents = contents.toArray(new String[contents.size()]);
					}
				}
			}
			return TreeJdbc.internLimit(count, this.contents);
		}
		return this.searchLocalIntern(entry, count, null, sort, -1L, -1L);
	}

	final String[] getChildren(final BaseEntry<?> entry,
			final int count,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter) {
		
		if (sort == null || sort.isBlank() || "alphabet".equals(sort)) {
			final List<String> contents = new ArrayList<>();
			final TreeJdbcEntry[] entries = this.entries;
			final int entryCount = entries.length;
			if (count == 0) {
				for (int i = 0; i < entryCount; ++i) {
					final TreeJdbcEntry current = entries[i];
					if (TreeJdbc.getLetter(alphabetConversion, defaultLetter, current.letter).equals(filterLetter)) {
						contents.add(current.lnkId);
					}
				}
			} else {
				int collected = 0;
				for (int i = 0; i < entryCount; ++i) {
					final TreeJdbcEntry current = entries[i];
					if (TreeJdbc.getLetter(alphabetConversion, defaultLetter, current.letter).equals(filterLetter)) {
						contents.add(current.lnkId);
						if (++collected >= count) {
							break;
						}
					}
				}
			}
			return contents.toArray(new String[contents.size()]);
		}
		return this.searchLocalIntern(entry, count, null, sort, alphabetConversion, defaultLetter, filterLetter);
	}

	final String[] getChildrenListable(final BaseEntry<?> entry, final int count, final String sort) {
		
		if (sort == null || sort.isBlank()) {
			return TreeJdbc.internLimit(count, this.internChildrenListable());
		}
		final String order = TreeJdbc.getListingSort(entry, sort);
		if (order == TreeJdbc.TITLE_ASC_NS || order == TreeJdbc.TITLE_ASC_VS || "alphabet".equals(sort)) {
			return TreeJdbc.internLimit(count, this.internChildrenListable());
		}
		if (order == TreeJdbc.CDATE_ASC) {
			return TreeJdbc.internLimit(count, this.internChildrenListableLog());
		}
		if (order == TreeJdbc.CDATE_DESC) {
			return TreeJdbc.internLimit(count, this.internChildrenListableHistory());
		}
		if (TreeJdbc.CDATE_ASC.equals(order)) {
			return TreeJdbc.internLimit(count, this.internChildrenListableLog());
		}
		if (TreeJdbc.CDATE_DESC.equals(order)) {
			return TreeJdbc.internLimit(count, this.internChildrenListableHistory());
		}
		if (TreeJdbc.TITLE_ASC_NS.equals(order) || TreeJdbc.TITLE_ASC_VS.equals(order)) {
			return TreeJdbc.internLimit(count, this.internChildrenListable());
		}
		return this.searchLocalIntern(entry, count, " AND o.objState in (2,4)", sort, -1L, -1L);
	}

	final String[] getChildrenListable(final BaseEntry<?> entry,
			final int count,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter) {
		
		if (sort == null || sort.isBlank() || "alphabet".equals(sort)) {
			final List<String> contents = new ArrayList<>();
			final TreeJdbcEntry[] entries = this.entries;
			final int entryCount = entries.length;
			if (count == 0) {
				for (int i = 0; i < entryCount; ++i) {
					final TreeJdbcEntry current = entries[i];
					if (current.listable && TreeJdbc.getLetter(alphabetConversion, defaultLetter, current.letter).equals(filterLetter)) {
						contents.add(current.lnkId);
					}
				}
			} else {
				int collected = 0;
				for (int i = 0; i < entryCount; ++i) {
					final TreeJdbcEntry current = entries[i];
					if (current.listable && TreeJdbc.getLetter(alphabetConversion, defaultLetter, current.letter).equals(filterLetter)) {
						contents.add(current.lnkId);
						if (++collected >= count) {
							break;
						}
					}
				}
			}
			return contents.toArray(new String[contents.size()]);
		}
		return this.searchLocalIntern(entry, count, null, sort, alphabetConversion, defaultLetter, filterLetter);
	}

	private final String[] getChildrenSearchable(final BaseEntry<?> entry, final int count, final String sort) {
		
		if (sort == null || sort.isBlank()) {
			if (this.contentsSearchable == null) {
				synchronized (this.entries) {
					if (this.contentsSearchable == null) {
						final List<String> contentsSearchable = new ArrayList<>();
						final TreeJdbcEntry[] entries = this.entries;
						final int entryCount = entries.length;
						for (int i = 0; i < entryCount; ++i) {
							final TreeJdbcEntry current = entries[i];
							if (current.searchable) {
								contentsSearchable.add(current.lnkId);
							}
						}
						this.contentsSearchable = contentsSearchable.toArray(new String[contentsSearchable.size()]);
					}
				}
			}
			return TreeJdbc.internLimit(count, this.contentsSearchable);
		}
		return this.searchLocalIntern(entry, count, " AND o.objState in (2,5)", sort, -1L, -1L);
	}

	final String[] getFiles(final BaseEntry<?> entry, final int count, final String sort) {
		
		if (sort == null || sort.isBlank() || "alphabet".equals(sort)) {
			if (this.files == null) {
				synchronized (this.entries) {
					if (this.files == null) {
						final List<String> files = new ArrayList<>();
						final TreeJdbcEntry[] entries = this.entries;
						final int entryCount = entries.length;
						for (int i = 0; i < entryCount; ++i) {
							final TreeJdbcEntry current = entries[i];
							if (!current.folder) {
								files.add(current.lnkId);
							}
						}
						this.files = files.toArray(new String[files.size()]);
					}
				}
			}
			return TreeJdbc.internLimit(count, this.files);
		}
		return this.searchLocalIntern(entry, count, " AND t.lnkFolder='N'", sort, -1L, -1L);
	}

	final String[] getFilesListable(final BaseEntry<?> entry, final int count, final String sort) {
		
		if (sort == null || sort.isBlank() || "alphabet".equals(sort)) {
			if (this.filesListable == null) {
				synchronized (this.entries) {
					if (this.filesListable == null) {
						final List<String> filesListable = new ArrayList<>();
						final TreeJdbcEntry[] entries = this.entries;
						final int entryCount = entries.length;
						for (int i = 0; i < entryCount; ++i) {
							final TreeJdbcEntry current = entries[i];
							if (current.listable && !current.folder) {
								filesListable.add(current.lnkId);
							}
						}
						this.filesListable = filesListable.toArray(new String[filesListable.size()]);
					}
				}
			}
			return TreeJdbc.internLimit(count, this.filesListable);
		}
		return this.searchLocalIntern(entry, count, " AND t.lnkFolder='N' AND o.objState in (2,4)", sort, -1L, -1L);
	}

	final String[] getFolders(final BaseEntry<?> entry, final int count, final String sort) {
		
		if (sort == null || sort.isBlank() || "alphabet".equals(sort)) {
			if (this.folders == null) {
				synchronized (this.entries) {
					if (this.folders == null) {
						final List<String> folders = new ArrayList<>();
						final TreeJdbcEntry[] entries = this.entries;
						final int entryCount = entries.length;
						for (int i = 0; i < entryCount; ++i) {
							final TreeJdbcEntry current = entries[i];
							if (current.folder) {
								folders.add(current.lnkId);
							}
						}
						this.folders = folders.toArray(new String[folders.size()]);
					}
				}
			}
			return TreeJdbc.internLimit(count, this.folders);
		}
		return this.searchLocalIntern(entry, count, " AND t.lnkFolder='Y'", sort, -1L, -1L);
		// return this.searchLocalIntern(null, 0, " AND t.lnkFolder='Y'",
		// "alphabet", -1L, -1L);
	}

	final String[] getFoldersListable() {
		
		if (this.foldersListable == null) {
			synchronized (this.entries) {
				if (this.foldersListable == null) {
					final List<String> foldersListable = new ArrayList<>();
					final TreeJdbcEntry[] entries = this.entries;
					final int entryCount = entries.length;
					for (int i = 0; i < entryCount; ++i) {
						final TreeJdbcEntry current = entries[i];
						if (current.listable && current.folder) {
							foldersListable.add(current.lnkId);
						}
					}
					this.foldersListable = foldersListable.toArray(new String[foldersListable.size()]);
				}
			}
		}
		return this.foldersListable;
		// return this.searchLocalIntern(null, 0, " AND t.lnkFolder='Y' AND
		// o.objState in (2,4)", "alphabet", -1L, -1L);
	}

	private final String[] internChildrenListable() {
		
		if (this.contentsListable == null) {
			synchronized (this.entries) {
				if (this.contentsListable == null) {
					final List<String> contentsListable = new ArrayList<>();
					final TreeJdbcEntry[] entries = this.entries;
					final int entryCount = entries.length;
					for (int i = 0; i < entryCount; ++i) {
						final TreeJdbcEntry current = entries[i];
						if (current.listable) {
							contentsListable.add(current.lnkId);
						}
					}
					this.contentsListable = contentsListable.toArray(new String[contentsListable.size()]);
				}
			}
		}
		return this.contentsListable;
	}

	private final String[] internChildrenListableHistory() {
		
		if (this.contentsListableHistory == null) {
			synchronized (this.entries) {
				if (this.contentsListableHistory == null) {
					final List<String> contentsListableHistory = new ArrayList<>();
					final TreeJdbcEntry[] entries = this.internGetEntriesHistory();
					final int entryCount = entries.length;
					for (int i = 0; i < entryCount; ++i) {
						final TreeJdbcEntry current = entries[i];
						if (current.listable) {
							contentsListableHistory.add(current.lnkId);
						}
					}
					this.contentsListableHistory = contentsListableHistory.toArray(new String[contentsListableHistory.size()]);
				}
			}
		}
		return this.contentsListableHistory;
	}

	private final String[] internChildrenListableLog() {
		
		if (this.contentsListableLog == null) {
			synchronized (this.entries) {
				if (this.contentsListableLog == null) {
					final List<String> contentsListableLog = new ArrayList<>();
					final TreeJdbcEntry[] entries = this.internGetEntriesLog();
					final int entryCount = entries.length;
					for (int i = 0; i < entryCount; ++i) {
						final TreeJdbcEntry current = entries[i];
						if (current.listable) {
							contentsListableLog.add(current.lnkId);
						}
					}
					this.contentsListableLog = contentsListableLog.toArray(new String[contentsListableLog.size()]);
				}
			}
		}
		return this.contentsListableLog;
	}

	private final TreeJdbcEntry[] internGetEntriesHistory() {
		
		if (this.entriesHistory == null) {
			synchronized (this.entries) {
				if (this.entriesHistory == null) {
					final TreeJdbcEntry[] entriesHistory = new TreeJdbcEntry[this.entries.length];
					System.arraycopy(this.entries, 0, entriesHistory, 0, entriesHistory.length);
					Arrays.sort(entriesHistory, TreeJdbc.COMPARATOR_HISTORY);
					this.entriesHistory = entriesHistory;
				}
			}
		}
		return this.entriesHistory;
	}

	private final TreeJdbcEntry[] internGetEntriesLog() {
		
		if (this.entriesLog == null) {
			synchronized (this.entries) {
				if (this.entriesLog == null) {
					final TreeJdbcEntry[] entriesLog = new TreeJdbcEntry[this.entries.length];
					System.arraycopy(this.entries, 0, entriesLog, 0, entriesLog.length);
					Arrays.sort(entriesLog, TreeJdbc.COMPARATOR_LOG);
					this.entriesLog = entriesLog;
				}
			}
		}
		return this.entriesLog;
	}

	final Map.Entry<String, Object>[]
			search(final int limit, final boolean all, final long timeout, final String sort, final long dateStart, final long dateEnd, final String filter) {
		
		if (this.server == null) {
			return null;
		}
		final String sortReplace = sort == null
			? null
			: TreeJdbc.REPLACEMENT_SEARCH_SORT.get(sort);
		final ExecSearchProgram program = this.server.getFinder().search(
				this.guid,
				all,
				sortReplace == null
					? sort
					: sortReplace,
				dateStart,
				dateEnd,
				filter);
		if (program == null) {
			return null;
		}
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is inaccessible now!");
			}
			return program.executeAll(timeout, limit, conn);
		} catch (final Throwable e) {
			throw new RuntimeException(e);
		}
	}

	final Map<Integer, Map<Integer, Map<String, Number>>> searchCalendar(final boolean all, final long timeout, final long dateStart, final long dateEnd, final String filter) {
		
		final ExecSearchCalendarProgram program = this.server.getFinder().searchCalendar(this.guid, all, dateStart, dateEnd, filter);
		if (program == null) {
			return null;
		}
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is inaccessible now!");
			}
			return program.executeAll(timeout, conn);
		} catch (final Throwable e) {
			throw new RuntimeException(e);
		}
	}

	final String[] searchLocal(final BaseEntry<?> entry, final int limit, final boolean all, final String sort, final long startDate, final long endDate, final String query) {
		
		final String order;
		if (query == null || query.isBlank()) {
			if (startDate == -1L && endDate == -1L) {
				if (all) {
					return this.getChildren(entry, limit, sort);
				}
				return this.getChildrenSearchable(entry, limit, sort);
			}
			if (sort == null || sort.isBlank()) {
				return TreeJdbc.searchLocalJava(all, startDate, endDate, this.entries);
			}
			order = TreeJdbc.getListingSort(entry, sort);
			if (order == TreeJdbc.TITLE_ASC_NS || order == TreeJdbc.TITLE_ASC_VS) {
				return TreeJdbc.searchLocalJava(all, startDate, endDate, this.entries);
			}
			if (order == TreeJdbc.CDATE_ASC) {
				return TreeJdbc.searchLocalJava(all, startDate, endDate, this.internGetEntriesLog());
			}
			if (order == TreeJdbc.CDATE_DESC) {
				return TreeJdbc.searchLocalJava(all, startDate, endDate, this.internGetEntriesHistory());
			}
			if (TreeJdbc.CDATE_ASC.equals(order)) {
				return TreeJdbc.searchLocalJava(all, startDate, endDate, this.internGetEntriesLog());
			}
			if (TreeJdbc.CDATE_DESC.equals(order)) {
				return TreeJdbc.searchLocalJava(all, startDate, endDate, this.internGetEntriesHistory());
			}
			if (TreeJdbc.TITLE_ASC_NS.equals(order) || TreeJdbc.TITLE_ASC_VS.equals(order)) {
				return TreeJdbc.searchLocalJava(all, startDate, endDate, this.entries);
			}
		} else {
			order = TreeJdbc.getListingSort(entry, sort);
		}
		if (this.server == null) {
			return null;
		}
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is inaccessible now!");
			}
			final String where;
			final StringBuilder sql;
			where = SyntaxQuery.filterToWhere(null, null, TreeJdbc.REPLACEMENT_FIELDS_NS, TreeJdbc.REPLACEMENT_FILTER_CONDITIONS, false, query);
			sql = new StringBuilder(128).append("SELECT t.lnkId FROM ").append(this.server.getTnTree()).append(" t, ").append(this.server.getTnObjects())
					.append(" o WHERE t.cntLnkId=? AND t.objId=o.objId");
			if (!all) {
				sql.append(" AND o.objState in (2,5)");
			}
			if (startDate != -1) {
				sql.append(" AND o.objCreated>=?");
			}
			if (endDate != -1) {
				sql.append(" AND o.objCreated<?");
			}
			if (where != null && where.length() > 0) {
				sql.append(" AND (").append(where).append(')');
			}
			if (order != null) {
				sql.append(order);
			}
			try (final PreparedStatement ps = conn.prepareStatement(sql.toString(), java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)) {
				final List<String> result = new ArrayList<>();
				ps.setString(1, this.guid);
				int index = 2;
				if (startDate != -1) {
					ps.setTimestamp(index++, new Timestamp(startDate));
				}
				if (endDate != -1) {
					ps.setTimestamp(index++, new Timestamp(endDate));
				}
				ps.setMaxRows(limit);
				try (final ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						result.add(rs.getString(1));
					}
				}
				return result.toArray(new String[result.size()]);
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	final String[] searchLocal(final BaseEntry<?> entry,
			final int limit,
			final boolean all,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter,
			final String query) {
		
		final String order;
		if (query == null || query.isBlank()) {
			if (all) {
				return this.getChildren(entry, limit, sort, alphabetConversion, defaultLetter, filterLetter);
			}
			return this.getChildrenListable(entry, limit, sort, alphabetConversion, defaultLetter, filterLetter);
		}
		order = TreeJdbc.getListingSort(entry, sort);
		if (this.server == null) {
			return null;
		}
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is inaccessible now!");
			}
			final String where;
			final StringBuilder sql;
			where = SyntaxQuery.filterToWhere(null, null, TreeJdbc.REPLACEMENT_FIELDS_NS, TreeJdbc.REPLACEMENT_FILTER_CONDITIONS, false, query);
			sql = new StringBuilder(128).append("SELECT t.lnkId,o.objTitle FROM ").append(this.server.getTnTree()).append(" t, ").append(this.server.getTnObjects())
					.append(" o WHERE t.cntLnkId=? AND t.objId=o.objId");
			if (!all) {
				sql.append(" AND o.objState in (2,4)");
			}
			if (where != null && where.length() > 0) {
				sql.append(" AND (").append(where).append(')');
			}
			if (order != null) {
				sql.append(order);
			}
			try (final PreparedStatement ps = conn.prepareStatement(sql.toString(), java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)) {
				final List<String> result = new ArrayList<>();
				ps.setString(1, this.guid);
				try (final ResultSet rs = ps.executeQuery()) {
					if (limit == 0) {
						while (rs.next()) {
							final String guid = rs.getString(1);
							if (TreeJdbc.getLetter(alphabetConversion, defaultLetter, rs.getString(2)).equals(filterLetter)) {
								result.add(guid);
							}
						}
					} else {
						int collected = 0;
						while (rs.next()) {
							final String guid = rs.getString(1);
							if (TreeJdbc.getLetter(alphabetConversion, defaultLetter, rs.getString(2)).equals(filterLetter)) {
								result.add(guid);
								if (++collected >= limit) {
									break;
								}
							}
						}
					}
				}
				return result.toArray(new String[result.size()]);
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	final Map<String, Number> searchLocalAlphabet(final boolean all, final Map<String, String> alphabetConversion, final String defaultLetter, final String query) {
		
		if (query == null || query.isBlank()) {
			if (all) {
				if (this.alphabetAll == null) {
					synchronized (this.entries) {
						if (this.alphabetAll == null) {
							final Map<String, Number> alphabetAll = new TreeMap<>();
							final TreeJdbcEntry[] entries = this.entries;
							final int entryCount = entries.length;
							for (int i = 0; i < entryCount; ++i) {
								final TreeJdbcEntry current = entries[i];
								TreeJdbc.fillAlphabet(alphabetAll, alphabetConversion, defaultLetter, current.letter);
							}
							this.alphabetAll = alphabetAll;
						}
					}
				}
				return this.alphabetAll;
			}
			if (this.alphabetSearchable == null) {
				synchronized (this.entries) {
					if (this.alphabetSearchable == null) {
						final Map<String, Number> alphabetSearchable = new TreeMap<>();
						final TreeJdbcEntry[] entries = this.entries;
						final int entryCount = entries.length;
						for (int i = 0; i < entryCount; ++i) {
							final TreeJdbcEntry current = entries[i];
							if (current.listable) {
								TreeJdbc.fillAlphabet(alphabetSearchable, alphabetConversion, defaultLetter, current.letter);
							}
						}
						this.alphabetSearchable = alphabetSearchable;
					}
				}
			}
			return this.alphabetSearchable;
		}
		if (this.server == null) {
			return null;
		}
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			final Map<String, Number> result = new TreeMap<>();
			final String where;
			final StringBuilder sql;
			where = SyntaxQuery.filterToWhere(null, null, TreeJdbc.REPLACEMENT_FIELDS_NS, TreeJdbc.REPLACEMENT_FILTER_CONDITIONS, false, query);
			sql = new StringBuilder(128).append("SELECT t.lnkId,o.objTitle FROM ").append(this.server.getTnTree()).append(" t, ").append(this.server.getTnObjects())
					.append(" o WHERE t.cntLnkId=? AND t.objId=o.objId");
			if (!all) {
				sql.append(" AND o.objState in (2,4)");
			}
			if (where != null && where.length() > 0) {
				sql.append(" AND (").append(where).append(')');
			}
			try (final PreparedStatement ps = conn.prepareStatement(sql.toString(), java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, this.guid);
				try (final ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						final String title = rs.getString(2);
						TreeJdbc.fillAlphabet(result, alphabetConversion, defaultLetter, title);
					}
				}
			}
			return result;
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	final Map<Integer, Map<Integer, Map<String, Number>>> searchLocalCalendar(final boolean all, final long startDate, final long endDate, final String query) {
		
		if (query == null || query.isBlank()) {
			final Calendar calendar = Calendar.getInstance();
			if (startDate == -1L && endDate == -1L) {
				if (all) {
					if (this.calendarAll == null) {
						synchronized (this.entries) {
							if (this.calendarAll == null) {
								final Map<Integer, Map<Integer, Map<String, Number>>> calendarAll = new TreeMap<>();
								final TreeJdbcEntry[] entries = this.entries;
								final int entryCount = entries.length;
								for (int i = 0; i < entryCount; ++i) {
									final TreeJdbcEntry current = entries[i];
									TreeJdbc.fillStats(calendarAll, calendar, new Date(current.created), "created");
								}
								this.calendarAll = calendarAll;
							}
						}
					}
					return this.calendarAll;
				}
				if (this.calendarSearchable == null) {
					synchronized (this.entries) {
						if (this.calendarSearchable == null) {
							final Map<Integer, Map<Integer, Map<String, Number>>> calendarSearchable = new TreeMap<>();
							final TreeJdbcEntry[] entries = this.entries;
							final int entryCount = entries.length;
							for (int i = 0; i < entryCount; ++i) {
								final TreeJdbcEntry current = entries[i];
								if (current.searchable) {
									TreeJdbc.fillStats(calendarSearchable, calendar, new Date(current.created), "created");
								}
							}
							this.calendarSearchable = calendarSearchable;
						}
					}
				}
				return this.calendarSearchable;
			}
			final long compareEndDate = endDate == -1L
				? Long.MAX_VALUE
				: endDate;
			final Map<Integer, Map<Integer, Map<String, Number>>> result = new TreeMap<>();
			final TreeJdbcEntry[] entries = this.entries;
			final int entryCount = entries.length;
			if (all) {
				for (int i = 0; i < entryCount; ++i) {
					final TreeJdbcEntry current = entries[i];
					final long created = current.created;
					if (created >= startDate && created < compareEndDate) {
						TreeJdbc.fillStats(result, calendar, new Date(created), "created");
					}
				}
			} else {
				for (int i = 0; i < entryCount; ++i) {
					final TreeJdbcEntry current = entries[i];
					if (current.searchable) {
						final long created = current.created;
						if (created >= startDate && created < compareEndDate) {
							TreeJdbc.fillStats(result, calendar, new Date(created), "created");
						}
					}
				}
			}
			return result;
		}
		if (this.server == null) {
			return null;
		}
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			final Map<Integer, Map<Integer, Map<String, Number>>> result = new TreeMap<>();
			final Calendar calendar = Calendar.getInstance();
			final String where;
			final StringBuilder sql;
			where = SyntaxQuery.filterToWhere(null, null, TreeJdbc.REPLACEMENT_FIELDS_NS, TreeJdbc.REPLACEMENT_FILTER_CONDITIONS, false, query);
			sql = new StringBuilder(128).append("SELECT t.lnkId,o.objCreated FROM ").append(this.server.getTnTree()).append(" t, ").append(this.server.getTnObjects())
					.append(" o WHERE t.cntLnkId=? AND t.objId=o.objId");
			if (!all) {
				sql.append(" AND o.objState in (2,5)");
			}
			if (startDate != -1) {
				sql.append(" AND o.objCreated>=?");
			}
			if (endDate != -1) {
				sql.append(" AND o.objCreated<?");
			}
			if (where != null && where.length() > 0) {
				sql.append(" AND (").append(where).append(')');
			}
			try (final PreparedStatement ps = conn.prepareStatement(sql.toString(), java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, this.guid);
				int index = 2;
				if (startDate != -1) {
					ps.setTimestamp(index++, new Timestamp(startDate));
				}
				if (endDate != -1) {
					ps.setTimestamp(index++, new Timestamp(endDate));
				}
				try (final ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						final Date CDate = rs.getDate(2);
						TreeJdbc.fillStats(result, calendar, CDate, "created");
					}
				}
			}
			return result;
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private final String[] searchLocalIntern(final BaseEntry<?> entry, final int limit, final String condition, final String sort, final long startDate, final long endDate) {
		
		if (this.server == null) {
			return null;
		}
		final String order = TreeJdbc.getListingSort(entry, sort);
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is inaccessible now!");
			}
			final StringBuilder sql;
			sql = new StringBuilder(128).append("SELECT t.lnkId FROM ").append(this.server.getTnTree()).append(" t, ").append(this.server.getTnObjects())
					.append(" o WHERE t.cntLnkId=? AND t.objId=o.objId");
			if (condition != null) {
				sql.append(condition);
			}
			if (startDate != -1) {
				sql.append(" AND o.objCreated>=?");
			}
			if (endDate != -1) {
				sql.append(" AND o.objCreated<?");
			}
			if (order != null) {
				sql.append(order);
			}
			try (final PreparedStatement ps = conn.prepareStatement(sql.toString(), java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)) {
				final List<String> result = new ArrayList<>();
				ps.setString(1, this.guid);
				int index = 2;
				if (startDate != -1) {
					ps.setTimestamp(index++, new Timestamp(startDate));
				}
				if (endDate != -1) {
					ps.setTimestamp(index++, new Timestamp(endDate));
				}
				ps.setMaxRows(limit);
				try (final ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						result.add(rs.getString(1));
					}
				}
				return result.toArray(new String[result.size()]);
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private final String[] searchLocalIntern(final BaseEntry<?> entry,
			final int limit,
			final String condition,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter) {
		
		if (this.server == null) {
			return null;
		}
		final String order = TreeJdbc.getListingSort(entry, sort);
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("DBMS is inaccessible now!");
			}
			final StringBuilder sql;
			sql = new StringBuilder(128).append("SELECT t.lnkId,o.objTitle FROM ").append(this.server.getTnTree()).append(" t, ").append(this.server.getTnObjects())
					.append(" o WHERE t.cntLnkId=? AND t.objId=o.objId");
			if (condition != null) {
				sql.append(condition);
			}
			if (order != null) {
				sql.append(order);
			}
			try (final PreparedStatement ps = conn.prepareStatement(sql.toString(), java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)) {
				final List<String> result = new ArrayList<>();
				ps.setString(1, this.guid);
				try (final ResultSet rs = ps.executeQuery()) {
					if (limit == 0) {
						while (rs.next()) {
							final String guid = rs.getString(1);
							if (TreeJdbc.getLetter(alphabetConversion, defaultLetter, rs.getString(2)).equals(filterLetter)) {
								result.add(guid);
							}
						}
					} else {
						int collected = 0;
						while (rs.next()) {
							final String guid = rs.getString(1);
							if (TreeJdbc.getLetter(alphabetConversion, defaultLetter, rs.getString(2)).equals(filterLetter)) {
								result.add(guid);
								if (++collected >= limit) {
									break;
								}
							}
						}
					}
				}
				return result.toArray(new String[result.size()]);
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final String toString() {
		
		return "TreeJdbc{guid=" + this.guid + ", luid=" + this.luid + "}";
	}
}
