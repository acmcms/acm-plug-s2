/*
 * Created on 18.10.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.jdbc;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class TreeJdbcEntry {
	final String	lnkId;
	
	final String	letter;
	
	final boolean	listable;
	
	final boolean	searchable;
	
	final boolean	folder;
	
	final long		created;
	
	TreeJdbcEntry(final String lnkId,
			final String letter,
			final boolean listable,
			final boolean searchable,
			final boolean folder,
			final long created) {
		this.lnkId = lnkId;
		this.letter = letter;
		this.listable = listable;
		this.searchable = searchable;
		this.folder = folder;
		this.created = created;
	}
}
